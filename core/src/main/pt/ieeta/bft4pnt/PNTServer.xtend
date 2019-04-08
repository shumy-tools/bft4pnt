package pt.ieeta.bft4pnt

import java.security.KeyPair
import java.util.HashSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.crypto.SignatureHelper
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Error
import pt.ieeta.bft4pnt.msg.Get
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Party
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Replica
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update
import pt.ieeta.bft4pnt.spi.IExtension
import pt.ieeta.bft4pnt.spi.IStore
import pt.ieeta.bft4pnt.spi.PntDatabase

class PNTServer {
  static val logger = LoggerFactory.getLogger(PNTServer.simpleName)
  
  val String partyKey
  val KeyPair keys
  
  val PntDatabase db
  val MessageBroker broker
  
  val (Message)=>boolean authorizer // authorize client?
  
  public val Replicator replicator
  public val extensions = new ConcurrentHashMap<String, IExtension>
  
  new(KeyPair keys, String dbName, MessageBroker broker, (Message)=>boolean authorizer) {
    this.partyKey = KeyPairHelper.encode(keys.public)
    this.keys = keys
    
    this.db = PntDatabase.get(dbName)
    this.broker = broker
    
    this.authorizer = authorizer
    this.replicator = new Replicator(db, broker)
  }
  
  def isReady() { broker.ready }
  
  def void start() {
    broker.start([
      logger.info("PNT-READY on {}", '''«hostString»:«port»''')
    ], [ inetSource, msg |
      
      // clients do not send replies or errors, redirect to replicator
      if (msg.type === Message.Type.REPLY || msg.type === Message.Type.ERROR) {
        
        //TODO: adapt in order to accept replicas on quorum transition!
        // is it part of the current quorum?
        val q = db.store.currentQuorum
        if (q.contains(msg.source)) {
          logger.error("Not part of the quorum: ({})", msg)
          val reply = new Message(msg.record, Error.unauthorized("Not part of the quorum!"))
          reply.id =  msg.id
          
          broker.send(inetSource, reply)
          return;
        }
        
        if (replicator.active)
          replicator.reply(msg)
        return;
      }
      
      // is the client message authorized?
      if (!authorizer.apply(msg)) {
        logger.error("Non authorized: ({})", msg)
        val reply = new Message(msg.record, Error.unauthorized("Non authorized!"))
        reply.id =  msg.id
        
        broker.send(inetSource, reply)
        return;
      }
      
      // process client message
      val cs = db.store.getOrCreate(msg.record.udi)
      synchronized(cs) {
        handle(cs, msg, msg.body)[ reply |
          reply.id = msg.id
          broker.send(inetSource, reply)
        ]
      }
    ])
  }
  
  dispatch private def void handle(IStore cs, Message msg, Insert body, (Message)=>void reply) {
    val q = db.store.currentQuorum
    val party = q.getParty(partyKey)
    
    val ext = extensions.get(body.type)
    val error = ext?.checkInsert(cs, msg, body)
    if (error !== null) {
      logger.error("Extension constraint error (msg={})", error)
      reply.apply(new Message(msg.record, Error.constraint(error)))
      return;
    }
    
    val has = msg.data.has(msg.record.fingerprint)
    switch has {
      case Data.Status.NO: reply.apply(new Message(msg.record, Reply.noData(party)))
      case Data.Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving(party)))
      case Data.Status.YES: {
        // verify record fingerprint
        if (msg.record.fingerprint != msg.data.fingerprint) {
          logger.error("Invalid record fingerprint (msg={}, data={})", msg.record.fingerprint, msg.data.fingerprint)
          reply.apply(new Message(msg.record, Error.invalid("Invalid record fingerprint!")))
          return;
        }
        
        //TODO: verify slices?
        
        //execute insert extension
        if (ext === null || ext.insert(cs, msg))
          cs.insert(msg)
        
        val rep = msg.setLocalReplica(party, keys.private)
        reply.apply(new Message(msg.record, Reply.ack(party, rep.signature)))
      }
    }
  }
  
  dispatch private def void handle(IStore cs, Message msg, Propose body, (Message)=>void reply) {
    val q = db.store.currentQuorum
    val party = q.getParty(partyKey)
    
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (propose, rec={})", msg.record.fingerprint)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    // Parties only accept proposals if messages for all past fingerprints exist.
    if (body.index > record.lastIndex + 1) {
      logger.error("Non existent past commits (index={}, last={})", body.index, record.lastCommit)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent past commits!")))
      return;
    }
    
    //Commits can have concurrent proposals of higher rounds for the same value.
    val update = record.getCommit(body.index)
    if (update !== null) {
      val updateBody = update.body as Update
      if (updateBody.propose.fingerprint != body.fingerprint || updateBody.propose.round >= body.round) {
        reply.apply(update)
        return;
      }
    }
    
    // Proposals can only be overridden by other proposals of higher rounds.
    val current = record.vote
    if (current !== null) {
      val currentBody = current.body as Reply
      if (currentBody.propose.round >= body.round) {
        reply.apply(current)
        return;
      }
    }
    
    val vote = new Message(msg.record, Reply.vote(party, body))
    record.vote = vote
    reply.apply(vote)
  }
  
  dispatch private def void handle(IStore cs, Message msg, Update body, (Message)=>void reply) {
    val q = db.store.currentQuorum
    val party = q.getParty(partyKey)
    
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (update, rec={})", msg.record.fingerprint)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    val ext = extensions.get(record.type)
    val error = ext?.checkUpdate(cs, msg, body)
    if (error !== null) {
      logger.error("Extension constraint error (msg={})", error)
      reply.apply(new Message(msg.record, Error.constraint(error)))
      return;
    }
    
    // Conflicting commits can only be overridden by other commits of higher rounds.
    val update = record.getCommit(body.propose.index)
    if (update !== null) {
      val updateBody = update.body as Update
      if (updateBody.propose.fingerprint != body.propose.fingerprint && updateBody.propose.round >= body.propose.round) {
        reply.apply(update)
        return;
      }
    }
    
    // get the message quorum
    val msgQ = db.store.getQuorumAt(body.quorum)
    if (msgQ === null) {
      logger.error("Non existent quorum (rec={})", body.quorum)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent quorum!")))
      return;
    } 
    
    // Votes with valid signatures with the exact same content structure (F, C, Ik, Dk, Ra)?
    val parties = new HashSet<Integer>
    for (vote : body.votes) {
      parties.add(vote.party)
      
      // An update commit is only valid if it has (n−t) reply votes from different parties with exactly the same content structure (F, C, Ik, Dk, Ra).
      val rVote = Reply.vote(new Party(vote.party, body.quorum), body.propose)
      val vMsg = new Message(msg.version, msg.record, rVote)
      val data = Message.getSignedBlock(vMsg.write)
      
      val key = msgQ.getPartyKey(vote.party)
      if (key === null) {
        logger.error("Invalid party (party={})", vote.party)
        reply.apply(new Message(msg.record, Error.invalid("Invalid party!")))
        return;
      }
      
      if (!SignatureHelper.verify(key, data, vote.signature)) {
        logger.error("Invalid vote (party={}, key={})", vote.party, KeyPairHelper.encode(key))
        reply.apply(new Message(msg.record, Error.invalid("Invalid vote!")))
        return;
      }
    }
    
    // (n−t) reply votes from different parties?
    if (parties.size < (msgQ.n - msgQ.t)) {
      logger.error("Not enough votes (v={}, n={}, t={})", parties.size, msgQ.n, msgQ.t)
      reply.apply(new Message(msg.record, Error.invalid("Not enough votes!")))
      return;
    }
    
    // Proposals can be overridden by commits of the same or higher rounds, or (n−t) commits from lower rounds.
    val current = record.vote
    if (current !== null) {
      val currentBody = current.body as Reply
      
      // Proposals can only be overridden by commits of the same or higher rounds
      if (currentBody.propose.round > body.propose.round &&
        (currentBody.propose.index != body.propose.index || currentBody.propose.fingerprint != body.propose.fingerprint) //Cannot directly accept commits for different proposals
      ) {
        //has (n - t) commits to override?
        val counts = msg.countReplicas(q)[
          val pQuorum = db.store.getQuorumAt(quorum)
          pQuorum.getPartyKey(index)
        ]
        
        // counts are relative to the current quorum, not the message quorum
        if (counts < (q.n - q.t)) {
          reply.apply(current)
          return;
        }
      }
    }
    
    // Parties only accept proposals if the data for the current fingerprint is safe in the local storage.
    val has = msg.data.has(body.propose.fingerprint)
    switch has {
      case Data.Status.NO: reply.apply(new Message(msg.record, Reply.noData(party, body.propose)))
      case Data.Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving(party, body.propose)))
      case Data.Status.YES: {
        // verify update fingerprint
        if (body.propose.fingerprint != msg.data.fingerprint) {
          logger.error("Invalid update fingerprint (msg={}, data={})", body.propose.fingerprint, msg.data.fingerprint)
          reply.apply(new Message(msg.record, Error.invalid("Invalid update fingerprint!")))
          return;
        }
        
        //TODO: verify slices?
        
        //execute update extension
        if (ext === null || ext.update(cs, msg))
          record.update(msg)
        
        val rep = msg.setLocalReplica(party, keys.private)
        reply.apply(new Message(msg.record, Reply.ack(party, body.propose, rep.signature)))
      }
    }
  }
  
  dispatch private def void handle(IStore cs, Message msg, Get body, (Message)=>void reply) {
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (get, rec={})", msg.record.fingerprint)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    val commit = record.getCommit(body.index)
    if (commit === null) {
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    reply.apply(commit)
  }
}

@FinalFieldsConstructor
class Replicator {
  static val logger = LoggerFactory.getLogger(Replicator.simpleName)
  
  val PntDatabase db
  val MessageBroker broker
  
  var Thread job = null
  val interval = new AtomicInteger(10000)
  
  def setInterval(int secods) {
    interval.set(1000 * secods)
  }
  
  synchronized def isActive() { job !== null }
  
  synchronized def void start() {
    job = new Thread[
      replicate
      Thread.sleep(interval.get)
    ]
    
    job.start
  }
  
  synchronized def void stop() {
    if (job !== null) {
      job.interrupt
      job = null
    }
  }
  
  synchronized def void replicate() {
    val q = db.store.currentQuorum
    val nMt = q.n - q.t
    for(party : q.allParties)
      db.store.pendingReplicas(nMt).forEach[
        val inet = q.getPartyAddress(party.index)
        broker.send(inet, it)
      ]
  }
  
  package def void reply(Message msg) {
    if (msg.type !== Message.Type.REPLY) {
      logger.error("Replication error: {}", msg)
      return;
    }
    
    val reply = (msg.body as Reply)
    val q = db.store.getQuorumAt(reply.party.quorum)
    val key = q.getPartyKey(reply.party.index)
    
    switch reply.type {
      case NO_DATA: {} //TODO: send the data
      case RECEIVING: {} //TODO: wait and retry (set a retry count?)
      case ACK: {
        // doesn't need to verify authorization since the message is from a party from the quorum.
        val cs = db.store.getOrCreate(msg.record.udi)
        synchronized(cs) {
          val rec = cs.getRecord(msg.record.fingerprint)
          val stored = if (reply.propose === null) rec.getCommit(0) else rec.getCommit(reply.propose.index)
          val rep = new Replica(stored.sigSlice, reply.party, reply.replica)
          
          // the signature will automatically verify if the replica corresponds to the stored record.
          if (rep.verifySignature(key))
            stored.addReplica(rep)
        }
      }
    }
  }
}

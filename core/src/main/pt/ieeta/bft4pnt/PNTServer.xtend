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
import pt.ieeta.bft4pnt.spi.IStoreManager
import pt.ieeta.bft4pnt.msg.Quorum
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.Set
import pt.ieeta.bft4pnt.msg.Slices

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
    
    start
  }
  
  def isReady() { broker.ready }
  
  def void start() {
    broker.start[ inetSource, msg |
      
      // clients do not send replies or errors, redirect to replicator
      if (msg.type === Message.Type.REPLY || msg.type === Message.Type.ERROR) {
        
        //TODO: adapt to accept parties from the last quorum on quorum transition
        // is the reply party part of the current quorum?
        val q = db.store.currentQuorum
        if (!q.contains(msg.source)) {
          logger.error("Not part of the quorum: {}", msg.source)
          return;
        }
        
        if (replicator.active)
          replicator.reply(msg)
        return;
      }
      
      // is the message authorized?
      if (!authorizer.apply(msg)) {
        logger.error("Non authorized: {}", msg)
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
    ]
  }
  
  dispatch private def void handle(IStore cs, Message msg, Insert body, (Message)=>void reply) {
    val record = cs.getRecord(msg.record.fingerprint)
    val q = if (record === null && msg.record.udi == IStoreManager.localStore && body.type == IStoreManager.quorumAlias) {
      db.store.setAlias(msg.record.fingerprint, IStoreManager.quorumAlias)
      msg.data.get(Quorum)
    } else
      db.store.currentQuorum
    
    // the record may already exist in a replication process or client re-submission
    val party = q.getParty(partyKey)
    if (record !== null) {
      logger.info("Record already exists: {}", msg.record.fingerprint)
      
      // proceed to up-date the local representation of the commit message with the new replicas of the received message
      val stored = record.getCommit(0)
      q.copyReplicas(msg, stored)
      
      val myRep = stored.replicas.findFirst[ rep | rep.party == party]
      reply.apply(new Message(msg.record, Reply.ack(party, myRep.signature)))
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
        
        // verify slices
        if (!body.slices.areSlicesOK(msg.data)) {
          logger.error("Invalid slice fingerprints!")
          reply.apply(new Message(msg.record, Error.invalid("Invalid slice fingerprints!")))
          return;
        }
        
        // check extension constraints
        val ext = extensions.get(body.type)
        val error = ext?.checkInsert(cs, msg, body)
        if (error !== null) {
          logger.error("Extension constraint error (msg={})", error)
          reply.apply(new Message(msg.record, Error.constraint(error)))
          return;
        }
        
        // execute insert extension
        val rep = msg.setLocalReplica(party, keys.private)
        if (ext === null || ext.insert(cs, msg))
          cs.insert(msg)
        
        reply.apply(new Message(msg.record, Reply.ack(party, rep.signature)))
      }
    }
  }
  
  dispatch private def void handle(IStore cs, Message msg, Propose body, (Message)=>void reply) {
    val q = db.store.currentQuorum
    val party = q.getParty(partyKey)
    
    // record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (propose, rec={})", msg.record.fingerprint)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    // parties only accept proposals if messages for all past fingerprints exist.
    if (body.index > record.lastIndex + 1) {
      logger.error("Non existent past commits (index={}, last={})", body.index, record.lastCommit)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent past commits!")))
      return;
    }
    
    // commits can have concurrent proposals of higher rounds for the same value.
    val update = record.getCommit(body.index)
    if (update !== null) {
      val updateBody = update.body as Update
      if (updateBody.propose.fingerprint != body.fingerprint || updateBody.propose.round >= body.round) {
        reply.apply(update)
        return;
      }
    }
    
    // proposals can only be overridden by other proposals of higher rounds.
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
    
    // record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (update, rec={})", msg.record.fingerprint)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    // get the message quorum
    val msgQ = db.store.getQuorumAt(body.quorum)
    if (msgQ === null) {
      logger.error("Non existent quorum (rec={})", body.quorum)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent quorum!")))
      return;
    } 
    
    // votes with valid signatures with the exact same content structure?
    val parties = new HashSet<Integer>
    for (vote : body.votes) {
      parties.add(vote.party)
      
      // an update commit is only valid if it has (n−t) reply votes from different parties with exactly the same content structure (F, C, Ik, Dk, Ra).
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
    
    // conflicting commits can only be overridden by other commits of higher rounds.
    val update = record.getCommit(body.propose.index)
    if (update !== null) {
      val updateBody = update.body as Update
      
      // update may already exist in a replication process or client re-submission
      if (updateBody.propose.fingerprint == body.propose.fingerprint && updateBody.propose.round === body.propose.round) {
        logger.info("Update already exists: (index={}, fingerprint={}, round={})", updateBody.propose.index, updateBody.propose.fingerprint, updateBody.propose.round)
        
        // proceed to up-date the local representation of the commit message with the new replicas of the received message
        q.copyReplicas(msg, update)
        
        val myRep = update.replicas.findFirst[ rep | rep.party == party]
        reply.apply(new Message(msg.record, Reply.ack(party, body.propose, myRep.signature)))
        return;
      } else if (updateBody.propose.round >= body.propose.round) {
        // doesn't accept the commit even if the Dk fingerprint is the same. The correct round should be replicated otherwise the replica signature will fail.
        reply.apply(update)
        return;
      }
    } else { // ignore propose rules if it already has a commit that can be overridden
      // proposals can be overridden by commits of the same or higher rounds, or (n−t) commits from lower rounds.
      val current = record.vote
      if (current !== null) {
        val currentBody = current.body as Reply
        
        // proposals can only be overridden by commits of the same or higher rounds
        if (currentBody.propose.round > body.propose.round &&
          (currentBody.propose.index != body.propose.index || currentBody.propose.fingerprint != body.propose.fingerprint) //Cannot directly accept commits for different proposals
        ) {
          // (n - t) replicas in the current quorum define a finalized commit and can override any propose rules
          // msg.countReplicas verify each replica signature. No need for further verifications.
          val counts = msg.countReplicas(party, db.store) + 1 // ignore the possibility of existing a local replica and then count with itself
          if (counts < (q.n - q.t)) {
            reply.apply(current)
            return;
          }
        }
      }
    }
    
    // parties only accept proposals if the data for the current fingerprint is safe in the local storage.
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
        
        // verify slices
        if (!body.slices.areSlicesOK(msg.data)) {
          logger.error("Invalid slice fingerprints!")
          reply.apply(new Message(msg.record, Error.invalid("Invalid slice fingerprints!")))
          return;
        }
        
        // check extension constraints
        val ext = extensions.get(record.type)
        val error = ext?.checkUpdate(cs, msg, body)
        if (error !== null) {
          logger.error("Extension constraint error (msg={})", error)
          reply.apply(new Message(msg.record, Error.constraint(error)))
          return;
        }
        
        // execute update extension
        val rep = msg.setLocalReplica(party, keys.private)
        if (ext === null || ext.update(cs, msg))
          record.update(msg)
        
        reply.apply(new Message(msg.record, Reply.ack(party, body.propose, rep.signature)))
      }
    }
  }
  
  dispatch private def void handle(IStore cs, Message msg, Get body, (Message)=>void reply) {
    // record must exist
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
  
  private def boolean areSlicesOK(Slices slices, Data data) {
    var index = 0
    for (slice : slices.slices) {
      if (data.sliceFingerprint(slices.size, index) != slice)
        return false
      index++
    }
    
    return true
  }
  
  private def Replica copyReplicas(Quorum current, Message from, Message to) {
    for (rep : from.replicas) {
      val key = current.getPartyKey(rep.party.index)
      if (key !== null && current.index === rep.party.quorum) {
        val thisRep = new Replica(to.sigSlice, rep.party, rep.signature)
        if (thisRep.verifySignature(key))
          to.addReplica(thisRep)
      }
    }
  } 
}

@FinalFieldsConstructor
class Replicator {
  static val logger = LoggerFactory.getLogger(Replicator.simpleName)
  
  val PntDatabase db
  val MessageBroker broker
  
  var Thread job = null
  
  val onReplicate = new AtomicReference<(Integer, Set<Integer>, Message)=>void>
  val onReply = new AtomicReference<(Reply)=>void>
  
  val active = new AtomicBoolean(false)
  val interval = new AtomicInteger(10000)
  
  def void setOnReplicate((Integer, Set<Integer>, Message)=>void onReplicate) {
    this.onReplicate.set = onReplicate
  }
  
  def void setOnReply((Reply)=>void onReply) {
    this.onReply.set = onReply
  }
  
  def void setInterval(int secods) { interval.set(1000 * secods) }
  def boolean isActive() { active.get }
  
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
      active.set = false
    }
  }
  
  synchronized def void replicate() {
    active.set = true
    val q = db.store.currentQuorum
    
    //TODO: on quorum transition it should confirm (n - t) replicas
    val pendings = db.store.pendingReplicas(q.n)
    
    
    // process and transmit should be separated procedures. The asynchronous replies from the transmission can change the msg.replicas
    for (msg : pendings) {
      try {
        // process pendings
        val toReplicate = new HashSet<Integer>
        for(party : q.allParties) {
          //TODO: ignore parties that are non-responsive?
          val key = q.getPartyKey(party.index)
          
          var isReplicated = false
          val iter = msg.replicas.iterator
          while (iter.hasNext && !isReplicated) {
            val rep = iter.next
            val repQ = db.store.getQuorumAt(rep.party.quorum)
            val repK = repQ.getPartyKey(rep.party.index)
            if (KeyPairHelper.encode(key) == KeyPairHelper.encode(repK) && rep.verifySignature(repK))
              isReplicated = true // already replicated
          }
          
          if (!isReplicated)
            toReplicate.add(party.index)
        }
        
        onReplicate.get?.apply(q.index, toReplicate, msg)
        
        // transmit pendings
        for(party : toReplicate) {
          logger.info("REPLICATE: {} -> {}", msg, party)
          val inet = q.getPartyAddress(party)
          broker.send(inet, msg)
        }
      } catch(Throwable ex) {
        ex.printStackTrace
        logger.error("Failed to replicate message: {}", msg)
      }
    }
  }
  
  package def void reply(Message msg) {
    if (msg.type !== Message.Type.REPLY) {
      logger.error("Replicator received an error message: {}", msg)
      return;
    }
    
    logger.info("REPLICA-REPLY: {}", msg)
    
    val reply = (msg.body as Reply)
    onReply.get?.apply(reply)
    
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
          
          // the signature will automatically verify if the replica corresponds to the stored record.
          val rep = new Replica(stored.sigSlice, reply.party, reply.replica)
          if (rep.verifySignature(key))
            stored.addReplica(rep)
        }
      }
      default: logger.info("REPLICA-IGNORED: {}", reply.type)
    }
  }
}

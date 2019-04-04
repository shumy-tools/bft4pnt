package pt.ieeta.bft4pnt

import java.util.HashMap
import java.util.HashSet
import java.util.Set
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
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Quorum
import pt.ieeta.bft4pnt.msg.Record
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update
import pt.ieeta.bft4pnt.spi.IExtension
import pt.ieeta.bft4pnt.spi.IRecord
import pt.ieeta.bft4pnt.spi.IStore
import pt.ieeta.bft4pnt.spi.IStoreManager

class PNTServer {
  static val logger = LoggerFactory.getLogger(PNTServer.simpleName)
  
  var IRecord quorum
  val Integer party
  
  val MessageBroker broker
  val IStoreManager storeMng
  
  val (Message)=>boolean authorizer // authorize client?
  
  public val Replicator replicator
  public val extensions = new HashMap<String, IExtension>
  
  // these values do not require persistence.
  // <UDI-F-Ik-Dk>
  val counts = new HashMap<String, Set<Integer>>
  
  new(Integer party, MessageBroker broker, IStoreManager storeMng, (Message)=>boolean authorizer) {
    this.party = party
    this.broker = broker
    this.storeMng = storeMng
    this.authorizer = authorizer
    this.replicator = new Replicator(this)
    
    val localStore = storeMng.local
    val qAlias = storeMng.alias("quorum")
    this.quorum = localStore.getRecord(qAlias)
  }
  
  private def Quorum getQuorumAt(int index) {
    val qMsg = quorum.getCommit(index)
    qMsg.data.get(Quorum)
  }
  
  private def int countReplicas(Message msg, Update update) {
    val key = '''«msg.record.udi»-«msg.record.fingerprint»-«update.propose.index»-«update.propose.fingerprint»'''
    val replicas = counts.get(key) ?: new HashSet<Integer>
    
    for (party : msg.replicaParties)
      replicas.add(party)
    
    return replicas.size
  }
  
  private def void clearReplicas(Message msg, Update update) {
    val key = '''«msg.record.udi»-«msg.record.fingerprint»-«update.propose.index»-«update.propose.fingerprint»'''
    counts.remove(key)
  }
  
  def isReady() { broker.ready }
  
  def void start() {
    broker.start([
      logger.info("PNT-READY {} on {}", '''«hostString»:«port»''')
      replicator.start
    ], [ inetSource, msg |
      // clients do not send replies or errors
      if (msg.type === Message.Type.REPLY || msg.type === Message.Type.ERROR) {
        replicator.reply(msg)
        return;
      }
      
      // is the message authorized?
      if (!authorizer.apply(msg)) {
        val reply = new Message(msg.record, Error.unauthorized("Non authorized UDI!"))
        reply.id =  msg.id
        
        broker.send(inetSource, reply)
        return;
      }
      
      val cs = storeMng.getOrCreate(msg.record.udi)
      synchronized(cs) {
        handle(cs, msg, msg.body)[ reply |
          reply.id = msg.id
          broker.send(inetSource, reply)
        ]
      }
    ])
  }
  
  dispatch private def void handle(IStore cs, Message msg, Insert body, (Message)=>void reply) {
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
        // verify fingerprints
        if (!msg.data.verify(msg.record.fingerprint, body.slices)) {
          reply.apply(new Message(msg.record, Error.invalid("Invalid fingerprints!")))
          return;
        }
        
        //execute insert extension
        if (ext === null || ext.insert(cs, msg))
          cs.insert(msg)
        
        replicator.ready(msg.record)
        reply.apply(new Message(msg.record, Reply.ack(party)))
      }
    }
  }
  
  dispatch private def void handle(IStore cs, Message msg, Propose body, (Message)=>void reply) {
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (rec={})", msg.record.fingerprint)
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
    
    val vote = new Message(msg.record, Reply.vote(party, quorum.lastIndex, body))
    record.vote = vote
    reply.apply(vote)
  }
  
  dispatch private def void handle(IStore cs, Message msg, Update body, (Message)=>void reply) {
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (rec={})", msg.record.fingerprint)
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
    
    val quorum = getQuorumAt(body.quorum)
    if (quorum === null) {
      logger.error("Non existent quorum (rec={})", body.quorum)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent quorum!")))
      return;
    }
    
    val nMt = quorum.n - quorum.t
    
    // Conflicting commits can only be overridden by other commits of higher rounds.
    val update = record.getCommit(body.propose.index)
    if (update !== null) {
      val updateBody = update.body as Update
      if (updateBody.propose.fingerprint != body.propose.fingerprint && updateBody.propose.round >= body.propose.round) {
        reply.apply(update)
        return;
      }
    }
    
    // Votes with valid signatures with the exact same content structure (F, C, Ik, Dk, Ra)?
    val parties = new HashSet<Integer>
    for (vote : body.votes) {
      parties.add(vote.party)
      
      // An update commit is only valid if it has (n−t) reply votes from different parties with exactly the same content structure (F, C, Ik, Dk, Ra).
      val rVote = Reply.vote(vote.party, body.quorum, body.propose)
      val vMsg = new Message(msg.version, Message.Type.REPLY, msg.record, rVote)
      val data = Message.getSignedBlock(vMsg.write)
      
      val key = quorum.getPartyKey(vote.party)
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
    if (parties.size < nMt) {
      logger.error("Not enough votes (v={}, n={}, t={}, (n-t)={})", parties.size, quorum.n, quorum.t, nMt)
      reply.apply(new Message(msg.record, Error.invalid("Not enough votes!")))
      return;
    }
    
    //verify replicas
    if (!msg.verifyReplicas(quorum)) {
      reply.apply(new Message(msg.record, Error.invalid("Invalid replicas!")))
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
        val counts = countReplicas(msg, body)
        if (counts < nMt) {
          reply.apply(current)
          return;
        }
      }
    }
    
    // Parties only accept proposals if the data for the current fingerprint is safe in the local storage.
    val has = msg.data.has(body.propose.fingerprint)
    switch has {
      case Data.Status.NO: reply.apply(new Message(msg.record, Reply.noData(party)))
      case Data.Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving(party)))
      case Data.Status.YES: {
        // verify fingerprints
        if (!msg.data.verify(body.propose.fingerprint, body.slices)) {
          reply.apply(new Message(msg.record, Error.invalid("Invalid fingerprints!")))
          return;
        }
        
        //execute update extension
        if (ext === null || ext.update(cs, msg))
          record.update(msg)
        
        clearReplicas(msg, body)
        replicator.ready(msg.record)
        reply.apply(new Message(msg.record, Reply.ack(party)))
      }
    }
  }
  
  dispatch private def void handle(IStore cs, Message msg, Get body, (Message)=>void reply) {
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (rec={})", msg.record.fingerprint)
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
  val PNTServer srv
  var Thread job = null 
  
  def isActive() { job !== null }
  
  def void start() {
    job = new Thread[
      
    ]
    
    job.start
  }
  
  def void stop() {
    if (job !== null) {
      job.interrupt
      job = null
    }
  }
  
  // signal the record as ready for replication
  def void ready(Record record) {
  }
  
  def void reply(Message msg) {
    //TODO: verify if is part of the quorum?
  }
}

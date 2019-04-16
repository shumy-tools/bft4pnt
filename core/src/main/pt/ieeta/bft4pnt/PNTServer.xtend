package pt.ieeta.bft4pnt

import java.security.KeyPair
import java.util.HashSet
import java.util.Set
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.crypto.SignatureHelper
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Error
import pt.ieeta.bft4pnt.msg.Get
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Quorum
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Signature
import pt.ieeta.bft4pnt.msg.Slices
import pt.ieeta.bft4pnt.msg.Update
import pt.ieeta.bft4pnt.spi.IExtension
import pt.ieeta.bft4pnt.spi.PntDatabase
import pt.ieeta.bft4pnt.spi.Store
import pt.ieeta.bft4pnt.spi.StoreManager

class PNTServer {
  static val logger = LoggerFactory.getLogger(PNTServer.simpleName)
  
  //val String party
  val KeyPair keys
  
  val PntDatabase db
  val MessageBroker broker
  
  val (Message)=>boolean authorizer // authorize client?
  
  public val Replicator replicator
  val extensions = new ConcurrentHashMap<String, IExtension>
  
  new(KeyPair keys, String dbName, MessageBroker broker, (Message)=>boolean authorizer) {
    //this.party = KeyPairHelper.encode(keys.public)
    this.keys = keys
    
    this.db = PntDatabase.get(dbName)
    this.broker = broker
    
    this.authorizer = authorizer
    this.replicator = new Replicator(db, broker)
    
    start
  }
  
  def isReady() { broker.ready }
  
  def void addExtension(String type, IExtension ext) {
    extensions.put(type, ext)
  }
  
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
  
  dispatch private def void handle(Store cs, Message msg, Insert body, (Message)=>void reply) {
    // get the quorum record
    val q = if (msg.record.udi == StoreManager.LOCAL_STORE && body.type == Store.QUORUM_ALIAS) {
      msg.data.get(Quorum)
    } else
      db.store.currentQuorum
    
    // handle all quorum records
    if (body.type == Store.QUORUM_ALIAS) {
      val qRec = cs.getFromAlias(Store.QUORUM_ALIAS)
      if (qRec !== null && msg.record.fingerprint != qRec) {
        logger.error("Quorum record already exists: (submitted={}, current={})", msg.record.fingerprint, qRec)
        reply.apply(new Message(msg.record, Error.unauthorized("Quorum record already exists!")))
        return;
      }
      
      //TODO: this constraint will block replication of messages from old quorums! Maybe we should have a differentiator of pure inserts from replicas!
      /*if (msg.record.udi != StoreManager.LOCAL_STORE && msg.data.integer !== q.index) {
        logger.error("Submitted quorum index is different from the current one: (submitted={}, current={})", msg.data.integer, q.index)
        reply.apply(new Message(msg.record, Error.unauthorized("Submitted quorum index is different from the current one!")))
        return;
      }*/
      
      // set the alias for the quorum record. For the global quorum and all stores.
      cs.setAlias(msg.record.fingerprint, Store.QUORUM_ALIAS)
    }
    
    // the record may already exist in a replication process or client re-submission
    val record = cs.getRecord(msg.record.fingerprint)
    if (record !== null) {
      logger.info("Record already exists: {}", msg.record.fingerprint)
      
      // proceed to up-date the local representation of the commit message with the new replicas of the received message
      val stored = record.getCommit(0)
      q.copyReplicas(msg, stored)
      
      val rep = stored.setLocalReplica(keys)
      reply.apply(new Message(msg.record, Reply.ack(q.index, keys.public, rep.signature)))
      return;
    }
    
    val has = msg.data.has(msg.record.fingerprint)
    switch has {
      case Data.Status.NO: reply.apply(new Message(msg.record, Reply.noData(q.index, keys.public)))
      case Data.Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving(q.index, keys.public)))
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
        
        // check extension constraints. LOCAL_STORE is a special store that is handled by the core protocol
        if (msg.record.udi != StoreManager.LOCAL_STORE) {
          val ext = extensions.get(body.type)
          val error = ext?.onInsert(cs, msg)
          if (error !== null) {
            logger.error("Extension constraint error (msg={})", error)
            reply.apply(new Message(msg.record, Error.constraint(error)))
            return;
          }
        }
        
        val rep = msg.setLocalReplica(keys)
        try {
          cs.insert(msg) // execute storage
        } catch (Throwable ex) {
          logger.error("Storage error!")
          ex.printStackTrace
          reply.apply(new Message(msg.record, Error.internal("Storage error!")))
          return
        }
        
        reply.apply(new Message(msg.record, Reply.ack(q.index, keys.public, rep.signature)))
      }
    }
  }
  
  dispatch private def void handle(Store cs, Message msg, Propose body, (Message)=>void reply) {
    val q = db.store.currentQuorum
    
    // record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      logger.error("Non existent record (propose, rec={})", msg.record.fingerprint)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
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
    
    // the local store has the quorum information (not the index)
    if (msg.record.udi != StoreManager.LOCAL_STORE)
      if (record.type == Store.QUORUM_ALIAS) {
        // accept the quorum index evolution if there are no pending replicas in the store 
        val pending = cs.numberOfPendingReplicas(q.n - q.t)
        if (pending !== 0) {
          logger.error("Pending replicas on this store: {}", pending)
          reply.apply(new Message(msg.record, Error.unauthorized("Pending replicas on this store!")))
          return;
        }
      } else if (q.index !== cs.quorumIndex) {
        // if the store is not in the current quorum index, it cannot accept the propose
        logger.error("Store in incorrect quorum: (index={}, store={})", q.index, cs.quorumIndex)
        reply.apply(new Message(msg.record, Error.unauthorized("Store in incorrect quorum!")))
        return;
      }
    
    // parties only accept proposals if messages for all past fingerprints exist.
    if (body.index > record.lastIndex + 1) {
      logger.error("Non existent past commits (index={}, last={})", body.index, record.lastCommit)
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent past commits!")))
      return;
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
    
    val vote = new Message(msg.record, Reply.vote(q.index, keys.public, body))
    record.vote = vote
    reply.apply(vote)
  }
  
  dispatch private def void handle(Store cs, Message msg, Update body, (Message)=>void reply) {
    val q = db.store.currentQuorum
    
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
    
    //TODO: this constraint will block replication of messages from old quorums! Maybe we should have a differentiator of pure inserts from replicas!
    /*if (msg.record.udi != StoreManager.LOCAL_STORE && record.type == Store.QUORUM_ALIAS && msg.data.integer !== q.index) {
      logger.error("Submitted quorum index is different from the current one: (submitted={}, current={})", msg.data.integer, q.index)
      reply.apply(new Message(msg.record, Error.unauthorized("Submitted quorum index is different from the current one!")))
      return;
    }*/
    
    // votes with valid signatures with the exact same content structure?
    val parties = new HashSet<String>
    for (vote : body.votes) {
      parties.add(vote.strSource)
      
      // an update commit is only valid if it has (n−t) reply votes from different parties with exactly the same content structure (F, C, Ik, Dk, Ra).
      val rVote = Reply.vote(body.quorum, vote.source, body.propose)
      val vMsg = new Message(msg.version, msg.record, rVote)
      val data = Message.getSignedBlock(vMsg.write)
      
      val key = msgQ.getPartyKey(vote.strSource)
      if (key === null) {
        logger.error("Invalid party (party={})", vote.strSource)
        reply.apply(new Message(msg.record, Error.invalid("Invalid party!")))
        return;
      }
      
      if (!SignatureHelper.verify(key, data, vote.signature)) {
        logger.error("Invalid vote (party={}, vote={})", vote.strSource, SignatureHelper.encode(vote.signature))
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
    val stored = record.getCommit(body.propose.index)
    if (stored !== null) {
      val storedBody = stored.body as Update
      
      // update may already exist in a replication process or client re-submission
      if (storedBody.propose.fingerprint == body.propose.fingerprint && storedBody.propose.round === body.propose.round) {
        logger.info("Update already exists: (index={}, fingerprint={}, round={})", storedBody.propose.index, storedBody.propose.fingerprint, storedBody.propose.round)
        
        // proceed to up-date the local representation of the commit message with the new replicas of the received message
        q.copyReplicas(msg, stored)
        
        val rep = stored.setLocalReplica(keys)
        reply.apply(new Message(msg.record, Reply.ack(body.quorum, keys.public, body.propose, rep.signature)))
        return;
      } else if (storedBody.propose.round >= body.propose.round) {
        // doesn't accept the commit even if the Dk fingerprint is the same. The correct round should be replicated otherwise the replica signature will fail.
        reply.apply(stored)
        return;
      }
    } else { // ignore propose rules if it already has a commit
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
          val counts = msg.countReplicas(keys.public, db.store) + 1 // ignore the possibility of existing a local replica and then count with itself
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
      case Data.Status.NO: reply.apply(new Message(msg.record, Reply.noData(body.quorum, keys.public, body.propose)))
      case Data.Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving(body.quorum, keys.public, body.propose)))
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
        
        // check extension constraints. LOCAL_STORE is a special store that is handled by the core protocol
        if (msg.record.udi != StoreManager.LOCAL_STORE) {
          val ext = extensions.get(record.type)
          val error = ext?.onUpdate(cs, msg)
          if (error !== null) {
            logger.error("Extension constraint error (msg={})", error)
            reply.apply(new Message(msg.record, Error.constraint(error)))
            return;
          }
        }
        
        val rep = msg.setLocalReplica(keys)
        try {
          record.update(msg) // execute storage
        } catch (Throwable ex) {
          logger.error("Storage error!")
          ex.printStackTrace
          reply.apply(new Message(msg.record, Error.internal("Storage error!")))
          return
        }
        
        reply.apply(new Message(msg.record, Reply.ack(body.quorum, keys.public, body.propose, rep.signature)))
      }
    }
  }
  
  dispatch private def void handle(Store cs, Message msg, Get body, (Message)=>void reply) {
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
  
  private def Signature copyReplicas(Quorum current, Message from, Message to) {
    for (rep : from.replicas) {
      if (current.contains(rep.strSource) && rep.verify(to.sigSlice))
        to.addReplica(rep)
    }
  } 
}

@FinalFieldsConstructor
class Replicator {
  static val logger = LoggerFactory.getLogger(Replicator.simpleName)
  
  val PntDatabase db
  val MessageBroker broker
  
  var Thread job = null
  
  val onReplicate = new AtomicReference<(Integer, Set<String>, Message)=>void>
  val onReply = new AtomicReference<(Reply)=>void>
  
  val active = new AtomicBoolean(false)
  val interval = new AtomicInteger(10000)
  
  def void setOnReplicate((Integer, Set<String>, Message)=>void onReplicate) {
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
    
    // process quorum state first
    val replicated = new HashSet<String>
    val qRec = db.store.local.getRecordFromAlias(Store.QUORUM_ALIAS)
    for (msg : qRec.history) {
      q.replicate(msg)
      replicated.add(msg.key)
    }
    
    // process and transmit should be separated procedures. The asynchronous replies from the transmission can change the msg.replicas
    val pendings = db.store.pendingReplicas(q.n)
    for (msg : pendings)
      if (!replicated.contains(msg.key))
        q.replicate(msg)
  }
  
  package def void reply(Message msg) {
    if (msg.type !== Message.Type.REPLY) {
      logger.error("Replicator received an error message: {}", msg)
      return;
    }
    
    logger.info("REPLICA-REPLY: {}", msg)
    
    val reply = (msg.body as Reply)
    onReply.get?.apply(reply)
    
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
          val rep = new Signature(reply.party, reply.replica)
          if (rep.verify(stored.sigSlice))
            stored.addReplica(rep)
        }
      }
      default: logger.info("REPLICA-IGNORED: {}", reply.type)
    }
  }
  
  private def void replicate(Quorum q, Message msg) {
    val toReplicate = new HashSet<String>
    for(party : q.allParties) {
      //TODO: ignore parties that are non-responsive?
      
      var isReplicated = false
      val iter = msg.replicas.iterator
      while (iter.hasNext && !isReplicated) {
        val rep = iter.next
        if (party == rep.strSource && rep.verify(msg.sigSlice))
          isReplicated = true // already replicated
      }
      
      if (!isReplicated)
        toReplicate.add(party)
    }
    
    onReplicate.get?.apply(q.index, toReplicate, msg)
    for(party : toReplicate) {
      try {
        logger.info("REPLICATE: {} -> {}", msg, party)
        val inet = q.getPartyAddress(party)
        broker.send(inet, msg)
      } catch(Throwable ex) {
        ex.printStackTrace
        logger.error("Failed to replicate quorum message: {}", msg)
      }
    }
  }
  
  private def String key(Message msg) {
    var key = '''«msg.record.udi»-«msg.record.fingerprint»'''
    if (msg.type === Message.Type.UPDATE) {
      val update = msg.body as Update
      key += '''-«update.quorum»-«update.propose.index»-«update.propose.fingerprint»-«update.propose.round»-«update.votes.size»'''
    }
    key
  }
}

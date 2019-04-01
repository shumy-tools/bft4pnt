package pt.ieeta.bft4pnt

import java.security.PublicKey
import java.util.HashMap
import java.util.HashSet
import java.util.Set
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.crypto.SignatureHelper
import pt.ieeta.bft4pnt.msg.Error
import pt.ieeta.bft4pnt.msg.Get
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update
import pt.ieeta.bft4pnt.spi.IClientStore
import pt.ieeta.bft4pnt.spi.IDataStore
import pt.ieeta.bft4pnt.spi.IExtension
import pt.ieeta.bft4pnt.spi.IStore

@FinalFieldsConstructor
class PNTServer {
  //static val logger = LoggerFactory.getLogger(PNTServer.simpleName)
  
  val MessageBroker broker
  val IStore store
  
  val (Message)=>boolean authorizer // authorize client store creation? Executed if there is no client store.
  val (Integer)=>PublicKey resolver
  
  public val extensions = new HashMap<String, IExtension>
  
  // these values do not require persistence.
  // <UDI-F-Ik-Dk>
  val counts = new HashMap<String, Set<Integer>>
  
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
  
  def void start() {
    broker.start[ msg |
      val cs = store.get(msg.record.udi) ?: {
        if (authorizer.apply(msg))
          store.create(msg.record.udi)
      }
        
      if (cs === null) {
        val reply = new Message(msg.record, Error.unauthorized("Non existent UDI!")) => [
          id = msg.id
          address = msg.address
        ]
        
        broker.send(reply)
        return;
      }
      
      synchronized(cs) {
        //TODO: verify source authorization (client or party)
        //msg.source
        
        //TODO: Parties should always verify the correctness of fingerprints after receiving the data block.
        handle(cs, msg, msg.body)[ reply |
          reply => [
            id = msg.id
            address = msg.address
          ]
          
          broker.send(reply)
        ]
      }
    ]
  }
  
  dispatch private def void handle(IClientStore cs, Message msg, Insert body, (Message)=>void reply) {
    val ext = extensions.get(body.type)
    val error = ext?.checkInsert(cs, msg, body)
    if (error !== null) {
      reply.apply(new Message(msg.record, Error.constraint(error)))
      return;
    }
    
    // is data directly in the message?
    if (msg.data !== null)
      cs.data.store(msg.record.fingerprint, msg.record.fingerprint, msg.data)
    
    val has = cs.data.has(msg.record.fingerprint, msg.record.fingerprint)
    switch has {
      case IDataStore.Status.NO: reply.apply(new Message(msg.record, Reply.noData))
      case IDataStore.Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving))
      case IDataStore.Status.YES: {
        // verify slices
        if (cs.data.verify(msg.record.fingerprint, msg.record.fingerprint, body.slices)) {
          reply.apply(new Message(msg.record, Error.invalid("Invalid slices!")))
          return;
        }
        
        //execute insert extension
        if (ext === null || ext.insert(cs, msg))
          cs.insert(msg)
        
        reply.apply(new Message(msg.record, Reply.ack))
      }
    }
  }
  
  dispatch private def void handle(IClientStore cs, Message msg, Propose body, (Message)=>void reply) {
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    val ext = extensions.get(record.type)
    val error = ext?.checkPropose(cs, msg, body)
    if (error !== null) {
      reply.apply(new Message(msg.record, Error.constraint(error)))
      return;
    }
    
    // Parties only accept proposals if messages for all past fingerprints exist.
    if (body.index === record.lastCommit + 1) {
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
    
    // is data directly in the message?
    if (msg.data !== null)
      cs.data.store(msg.record.fingerprint, body.fingerprint, msg.data)
    
    // Parties only accept proposals if the data for the current fingerprint is safe in the local storage.
    val has = cs.data.has(msg.record.fingerprint, body.fingerprint)
    switch has {
      case IDataStore.Status.NO: reply.apply(new Message(msg.record, Reply.noData))
      case IDataStore.Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving))
      case IDataStore.Status.YES: {
        val vote = new Message(msg.record, Reply.vote(store.quorum, body))
        record.vote = vote
        reply.apply(vote)
      }
    }
  }
  
  dispatch private def void handle(IClientStore cs, Message msg, Update body, (Message)=>void reply) {
    val conf = store.quorum
    val nMt = conf.n - conf.t
    
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
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
    
    // An update commit is only valid if it has (n−t) reply votes from different parties with exactly the same content structure (F, C, Ik, Dk, Ra).
    val rVote = Reply.vote(body.quorum, body.propose)
    val vMsg = new Message(msg.version, Message.Type.REPLY, msg.record, rVote)
    val data = Message.getSignedBlock(vMsg.write)
    
    // Votes with valid signatures with the exact same content structure (F, C, Ik, Dk, Ra)?
    val parties = new HashSet<String>
    for (vote : body.votes) {
      parties.add(vote.party)
      if (!SignatureHelper.verify(vote.key, data, vote.signature)) {
        reply.apply(new Message(msg.record, Error.invalid("Invalid vote!")))
        return;
      }
    }
    
    // (n−t) reply votes from different parties?
    if (parties.size < nMt) {
      reply.apply(new Message(msg.record, Error.invalid("Not enough votes!")))
      return;
    }
    
    //verify replicas
    if (!msg.verifyReplicas(resolver)) {
      reply.apply(new Message(msg.record, Error.invalid("Invalid replicas!")))
      return;
    }
    
    // Proposals can be overridden by commits of the same or higher rounds, or (n−t) commits from lower rounds.
    val current = record.vote
    if (current !== null) {
      val currentBody = current.body as Reply
      
      // Cannot directly accept a commit for a different proposal.
      // Proposals can only be overridden by commits of the same or higher rounds
      if (
        currentBody.propose.index != body.propose.index
        || currentBody.propose.fingerprint != body.propose.fingerprint
        || currentBody.propose.round > body.propose.round
      ) {
        //has (n - t) commits to override?
        val counts = countReplicas(msg, body)
        if (counts < nMt) {
          //TODO: the replication mechanism should have their own messages. The source of these cannot be properly identified!
          reply.apply(current)
          return;
        }
      }
    }
    
    // verify slices
    if (cs.data.verify(msg.record.fingerprint, body.propose.fingerprint, body.slices)) {
      reply.apply(new Message(msg.record, Error.invalid("Invalid slices!")))
      return;
    }
    
    //execute update extension
    val ext = extensions.get(record.type)
    if (ext === null || ext.update(cs, msg))
      record.update(msg)
    
    clearReplicas(msg, body)
    reply.apply(new Message(msg.record, Reply.ack))
  }
  
  dispatch private def void handle(IClientStore cs, Message msg, Get body, (Message)=>void reply) {
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    val index = if (body.index === -1) record.lastCommit else body.index
    val commit = record.getCommit(index)
    if (commit === null) {
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
      return;
    }
    
    reply.apply(commit)
  }
  
  dispatch private def void handle(IClientStore cs, Message msg, Reply body, (Message)=>void reply) {
    
  }
  
  dispatch private def void handle(IClientStore cs, Message msg, Error body, (Message)=>void reply) {
    
  }
}
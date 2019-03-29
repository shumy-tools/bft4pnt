package pt.ieeta.bft4pnt

import java.util.HashSet
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
import pt.ieeta.bft4pnt.store.IClientStore
import pt.ieeta.bft4pnt.store.IDataStore
import pt.ieeta.bft4pnt.store.IStore
import java.util.HashMap
import pt.ieeta.bft4pnt.msg.Record
import org.slf4j.LoggerFactory

@FinalFieldsConstructor
class PNTServer {
  static val logger = LoggerFactory.getLogger(PNTServer.simpleName)
  
  val MessageBroker broker
  val IStore store
  
  // the values do not require persistence.
  val counts = new HashMap<String, Integer>
  
  private def int count(String source, Record record, Update update) {
    val key = '''«source»-«record.udi»-«record.fingerprint»-«update.propose.index»-«update.propose.fingerprint»'''
    val value = counts.get(key) ?: 0
    counts.put(key, value + 1)
    return value + 1
  }
  
  private def void clear(String source, Record record, Update update) {
    val key = '''«source»-«record.udi»-«record.fingerprint»-«update.propose.index»-«update.propose.fingerprint»'''
    counts.remove(key)
  }
  
  def void start() {
    broker.start[ msg |
      val cs = store.get(msg.record.udi)
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
  
  dispatch private def void handle(IClientStore cs, Message msg, Propose body, (Message)=>void reply) {
    // Record must exist
    val record = cs.getRecord(msg.record.fingerprint)
    if (record === null) {
      reply.apply(new Message(msg.record, Error.unauthorized("Non existent record!")))
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
    
    // Parties only accept proposals if the data for the current fingerprint is safe in the local storage.
    val has = cs.data.has(msg.record.fingerprint, body.fingerprint)
    switch has {
      case pt.ieeta.bft4pnt.store.IDataStore$Status.NO: reply.apply(new Message(msg.record, Reply.noData))
      case pt.ieeta.bft4pnt.store.IDataStore$Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving))
      case pt.ieeta.bft4pnt.store.IDataStore$Status.YES: {
        val vote = new Message(msg.record, Reply.vote(store.quorum, body))
        record.vote = vote
        reply.apply(vote)
      }
    }
  }
  
  dispatch private def void handle(IClientStore cs, Message msg, Insert body, (Message)=>void reply) {
    val has = cs.data.has(msg.record.fingerprint, msg.record.fingerprint)
    switch has {
      case pt.ieeta.bft4pnt.store.IDataStore$Status.NO: reply.apply(new Message(msg.record, Reply.noData))
      case pt.ieeta.bft4pnt.store.IDataStore$Status.PENDING: reply.apply(new Message(msg.record, Reply.receiving))
      case pt.ieeta.bft4pnt.store.IDataStore$Status.YES: {
        // verify slices
        if (cs.data.verify(msg.record.fingerprint, msg.record.fingerprint, body.slices)) {
          reply.apply(new Message(msg.record, Error.invalid("Invalid slices!")))
          return;
        }
        
        cs.insert(msg)
        reply.apply(new Message(msg.record, Reply.ack))
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
    val vMsg = new Message(msg.version, Message.Type.VOTE, msg.record, rVote)
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
        val counts = count(msg.source, msg.record, body)
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
    
    // Accepted
    clear(msg.source, msg.record, body)
    record.update(msg)
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
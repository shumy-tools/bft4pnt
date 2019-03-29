package pt.ieeta.bft4pnt.store

import java.util.ArrayList
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import org.eclipse.xtend.lib.annotations.Accessors
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.QuorumConfig
import pt.ieeta.bft4pnt.msg.Update

class MemoryStore implements IStore {
  @Accessors var QuorumConfig quorum
  
  val clients = new ConcurrentHashMap<String, IClientStore>
  
  new(QuorumConfig quorum) {
    this.quorum = quorum
  }
  
  override get(String udi) { clients.get(udi) }
  
  override create(String udi) {
    clients.get(udi) ?: {
      val created = new MemoryClientStore
      clients.put(udi, created)
      created
    }
  }
}

class MemoryClientStore implements IClientStore {
  val records = new HashMap<String, IRecord>
  
  override insert(Message msg) {
    records.put(msg.record.fingerprint, new ClientRecord(msg))
  }
  
  override getRecord(String record) { records.get(record) }
  
  override getData() {
    throw new UnsupportedOperationException("TODO: auto-generated method stub")
  }
}

class ClientRecord implements IRecord {
  @Accessors var Message vote
  
  var last = 0
  val history = new ArrayList<Message>
  
  new(Message insert) {
    history.add(insert)
  }
  
  override lastCommit() { last }
  override getCommit(int index) { history.get(index) }
  
  override update(Message msg) {
    val update = msg.body as Update
    if (update.propose.index > history.size)
      throw new RuntimeException("Trying to update ahead!")
    
    if (update.propose.index == history.size)
      history.add(msg)
    else
      history.set(update.propose.index, msg)
    
    last = update.propose.index
  }
  
  override slices() {
    throw new UnsupportedOperationException("TODO: auto-generated method stub")
  }
}

package pt.ieeta.bft4pnt.spi

import java.util.ArrayList
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import org.eclipse.xtend.lib.annotations.Accessors
import pt.ieeta.bft4pnt.crypto.ArraySlice
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.QuorumConfig
import pt.ieeta.bft4pnt.msg.Slices
import pt.ieeta.bft4pnt.msg.Update
import java.util.Arrays

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
  val data = new ClientData
  
  override insert(Message msg) {
    records.put(msg.record.fingerprint, new ClientRecord(msg))
  }
  
  override getRecord(String record) { records.get(record) }
  override getData() { data }
}

class ClientRecord implements IRecord {
  @Accessors var Message vote
  
  var last = 0
  val history = new ArrayList<Message>
  
  new(Message insert) {
    history.add(insert)
  }
  
  override getType() {
    val insert = history.get(0).body as Insert
    insert.type
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

class ClientData implements IDataStore {
  val store = new HashMap<String, byte[]>
  
  override has(String record, String dk) {
    val key = '''«record»-«dk»'''
    if (store.get(key) === null)
      return Status.NO
    
    return Status.YES
  }
  
  override verify(String record, String dk, Slices slices) {
    true
  }
  
  override store(String record, String dk, byte[] data) {
    val key = '''«record»-«dk»'''
    val trg = Arrays.copyOf(data, data.length)
    store.put(key, trg)
  }
  
  override store(String record, String dk, ArraySlice slice) {
    val key = '''«record»-«dk»'''
    val trg = newByteArrayOfSize(slice.length)
    System.arraycopy(slice.data, slice.offset, trg, 0, slice.length)
    store.put(key, trg)
  }
}

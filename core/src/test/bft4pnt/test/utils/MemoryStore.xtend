package bft4pnt.test.utils

import java.util.ArrayList
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import org.eclipse.xtend.lib.annotations.Accessors
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Quorum
import pt.ieeta.bft4pnt.msg.Update
import pt.ieeta.bft4pnt.spi.IRecord
import pt.ieeta.bft4pnt.spi.IStore
import pt.ieeta.bft4pnt.spi.IStoreManager

class MemoryStoreManager implements IStoreManager {
  val alias = new ConcurrentHashMap<String, String>
  val clients = new ConcurrentHashMap<String, IStore>
  
  new(Quorum quorum) {
    val msg = Insert.create(0L, "local", "quorum", new Data(quorum))
    alias.put("quorum" , msg.record.fingerprint)
    local.insert(msg)
  }
  
  override alias(String alias) { this.alias.get(alias) }
  
  override local() { internalGetOrCreate("local") }
  
  override getOrCreate(String udi) {
    if (udi == "local")
      throw new RuntimeException("Reserved store.")
    
    internalGetOrCreate(udi)
  }
  
  private def internalGetOrCreate(String udi) {
    clients.get(udi) ?: {
      val created = new MemoryStore
      clients.put(udi, created)
      created
    }
  }
}

class MemoryStore implements IStore {
  val records = new HashMap<String, IRecord>
  
  override insert(Message msg) {
    val rec = new StoreRecord(msg)
    records.put(msg.record.fingerprint, rec)
    rec
  }
  
  override getRecord(String record) {
    records.get(record)
  }
}

class StoreRecord implements IRecord {
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
  
  override lastIndex() { last }
  override lastCommit() { getCommit(last) }
  
  override getCommit(int index) {
    if (index === -1) lastCommit
    if (index > last) return null
    
    history.get(index)
  }
  
  override update(Message msg) {
    val update = msg.body as Update
    if (update.propose.index > history.size)
      throw new RuntimeException("Trying to update ahead!")
    
    if (update.propose.index === history.size)
      history.add(msg)
    else
      history.set(update.propose.index, msg)
    
    last = update.propose.index
  }
}

package bft4pnt.test.utils

import java.util.ArrayList
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import org.eclipse.xtend.lib.annotations.Accessors
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Quorum
import pt.ieeta.bft4pnt.spi.IRecord
import pt.ieeta.bft4pnt.spi.IStore
import pt.ieeta.bft4pnt.spi.IStoreManager

class InMemoryStoreMng extends IStoreManager {
  val alias = new ConcurrentHashMap<String, String>
  val clients = new ConcurrentHashMap<String, IStore>
  
  new(Quorum quorum) {
    val msg = Insert.create(0L, "local", "quorum", new Data(quorum))
    alias.put("quorum" , msg.record.fingerprint)
    local.insert(msg)
  }
  
  override alias(String alias) { this.alias.get(alias) }
  
  override pendingReplicas(String party) {
    throw new UnsupportedOperationException("TODO: auto-generated method stub")
  }
  
  override internalGetOrCreate(String udi) {
    clients.get(udi) ?: {
      val created = new InMemoryStore
      clients.put(udi, created)
      created
    }
  }
}

class InMemoryStore implements IStore {
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

class StoreRecord extends IRecord {
  @Accessors var Message vote
  @Accessors var int lastIndex = 0
  @Accessors val history = new ArrayList<Message>
  
  new(Message insert) {
    history.add(insert)
  }
  
  override protected addHistory(Message msg) {
    history.add(msg)
  }
  
  override protected setHistory(int index, Message msg) {
    history.set(index, msg)
  }
  
}

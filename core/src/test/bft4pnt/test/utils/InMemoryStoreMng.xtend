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
import pt.ieeta.bft4pnt.msg.Party
import java.util.List
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

class InMemoryStoreMng extends IStoreManager {
  val repTable = new ConcurrentHashMap<Party, List<Message>>
  
  val alias = new ConcurrentHashMap<String, String>
  val stores = new ConcurrentHashMap<String, IStore>
  
  new(Quorum quorum) {
    val msg = Insert.create(0L, "local", "quorum", new Data(quorum))
    alias.put("quorum" , msg.record.fingerprint)
    local.insert(msg)
  }
  
  override alias(String alias) { this.alias.get(alias) }
  
  override pendingReplicas(Party party) {
    repTable.get(party)
  }
  
  override internalGetOrCreate(String udi) {
    stores.get(udi) ?: {
      val created = new InMemoryStore(this)
      stores.put(udi, created)
      created
    }
  }
}

@FinalFieldsConstructor
class InMemoryStore implements IStore {
  val InMemoryStoreMng mng
  val records = new HashMap<String, IRecord>
  
  override insert(Message msg) {
    val rec = new InMemoryRecord(mng, msg)
    records.put(msg.record.fingerprint, rec)
    rec
  }
  
  override getRecord(String record) {
    records.get(record)
  }
}

class InMemoryRecord extends IRecord {
  val InMemoryStoreMng mng
  
  @Accessors var Message vote
  @Accessors var int lastIndex = 0
  @Accessors val history = new ArrayList<Message>
  
  new(InMemoryStoreMng mng, Message insert) {
    this.mng = mng
    history.add(insert)
    updateRepTable(insert)
  }
  
  override protected addHistory(Message msg) {
    history.add(msg)
    updateRepTable(msg)
  }
  
  override protected setHistory(int index, Message msg) {
    history.set(index, msg)
    updateRepTable(msg)
  }
  
  private def void updateRepTable(Message msg) {
    for (rep : msg.replicas) {
      //TODO: update repTable?
    }
  }
}

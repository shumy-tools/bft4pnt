package bft4pnt.test.utils

import java.util.ArrayList
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.concurrent.ConcurrentHashMap
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Update
import pt.ieeta.bft4pnt.spi.Store
import pt.ieeta.bft4pnt.spi.StoreManager
import pt.ieeta.bft4pnt.spi.StoreRecord

// Real implementations should carefully craft this class. There are several optimizations that can be done here.
@FinalFieldsConstructor
class RepTable {
  val InMemoryStoreMng mng
  
  /* Real implementations should optimize the HashMap, it may get really big.
   * A good implementation should be based on a persistence HashMap with in memory cache, i.e. using SQL cursor implementations
   */
  val msgCounts = new HashMap<String, Integer>
  val records = new HashMap<Integer, Map<String, Message>>
  
  //get commits with less than "minimum" replicas for the current quorum
  synchronized def List<Message> get(int minimum) {
    val pendings = new HashMap<String, Message>
    for (n : 0 ..< minimum) {
      val recs = records.get(n)
      if (recs !== null)
        pendings.putAll(recs)
    }
    
    pendings.values.toList
  }
  
  synchronized def Integer get(String udi, int minimum) {
    get(minimum).filter[record.udi == udi].size
  }
  
  synchronized def void update(Message msg) {
    val key = msg.key
    val counts = msg.countReplicas(mng)
    
    // remove from the previous count.
    val previousCount = msgCounts.get(key) ?: 0
    if (counts > previousCount) {
      /*Real implementations may also maintain replicas from the current stored message, in case the new message is missing some replicas;
      this can happen if the replica comes from a different party.
      */
      getAt(previousCount).remove(key)
      getAt(counts).put(key, msg)
      msgCounts.put(key, counts)
    }
  }
  
  private def Map<String, Message> getAt(int minimum) {
    records.get(minimum) ?: {
      val newMap = new HashMap<String, Message>
      records.put(minimum, newMap)
      newMap
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

class InMemoryStoreMng extends StoreManager {
  val RepTable repTable
  val stores = new ConcurrentHashMap<String, Store>
  
  new() { repTable = new RepTable(this) }
  
  override getOrCreate(String udi) {
    stores.get(udi) ?: {
      val created = new InMemoryStore(repTable, udi)
      stores.put(udi, created)
      created
    }
  }
  
  override pendingReplicas(int minimum) {
    repTable.get(minimum)
  }
}

@FinalFieldsConstructor
class InMemoryStore extends Store {
  val RepTable repTable
  
  @Accessors val String udi
  
  val alias = new ConcurrentHashMap<String, String>
  val records = new HashMap<String, StoreRecord>
  
  override setAlias(String record, String alias) { this.alias.put(alias, record) }
  override getFromAlias(String alias) { this.alias.get(alias) }
  
  override insert(Message msg) {
    return new InMemoryStoreRecord(repTable) => [
      records.put(msg.record.fingerprint, it)
      addHistory(msg)
    ]
  }
  
  override getRecord(String record) {
    records.get(record)
  }
  
  override numberOfPendingReplicas(int minimum) {
    repTable.get(udi, minimum)
  }
}

@FinalFieldsConstructor
class InMemoryStoreRecord extends StoreRecord {
  val RepTable repTable
  
  @Accessors var Message vote
  @Accessors var int lastIndex = 0
  @Accessors val history = new ArrayList<Message>
  
  override protected addHistory(Message msg) {
    history.add(msg)
    repTable.update(msg)
  }
  
  override protected setHistory(int index, Message msg) {
    history.set(index, msg)
    repTable.update(msg)
  }
}

package pt.ieeta.bft4pnt.spi

import java.util.List
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Quorum
import pt.ieeta.bft4pnt.msg.Update

abstract class StoreManager {
  public static val LOCAL_STORE = "admin"
  
  synchronized def Quorum getCurrentQuorum() {
    val qRec = local.getRecordFromAlias(Store.QUORUM_ALIAS)
    getQuorumAt(qRec.lastIndex)
  }
  
  synchronized def Quorum getQuorumAt(int index) {
    val qRec = local.getRecordFromAlias(Store.QUORUM_ALIAS)
    
    val qMsg = qRec.getCommit(index)
    if (qMsg === null)
      throw new RuntimeException('''No quorum at index: «index»''')
    
    val q = qMsg.data.get(Quorum)
    if (q.index !== index)
      throw new RuntimeException('''Incorrect quorum index: («q.index» != «index»)''')
    
    return q
  }
  
  def local() { getOrCreate(LOCAL_STORE) }
  def Store getOrCreate(String udi)
  
  // all messages from all stores with a number of replicas less than the "minimum"
  def List<Message> pendingReplicas(int minimum)
}

abstract class Store {
  public static val QUORUM_ALIAS = "quorum"
  
  def Integer getQuorumIndex() {
    val record = getRecordFromAlias(QUORUM_ALIAS)
    record.lastCommit.data.integer
  }
  
  def StoreRecord getRecordFromAlias(String alias) {
    val record = getFromAlias(alias)
    if (record === null)
      throw new RuntimeException('''No record for alias: (udi=«udi», alias=«alias»)''')
    
    getRecord(record)
  }
  
  def String getUdi()
  
  def void setAlias(String record, String alias)
  def String getFromAlias(String alias)
  
  def StoreRecord insert(Message msg)
  def StoreRecord getRecord(String record)
  
  // number of message from this store with replicas less than the "minimum"
  def Integer numberOfPendingReplicas(int minimum)
}

abstract class StoreRecord {
  def getType() {
    val insert = history.get(0).body as Insert
    insert.type
  }
  
  def lastCommit() { getCommit(lastIndex) }
  
  def getCommit(int index) {
    if (index === -1) history.get(lastIndex)
    if (index > lastIndex) return null
    
    val msg = history.get(index)
    msg.addReplicaChangeListener("IStoreManager", [
      if (msg.type === Message.Type.INSERT)
        setHistory(0, msg)
      else
        setHistory((msg.body as Update).propose.index, msg)
    ])
    
    return msg
  }
  
  def update(Message msg) {
    val hist = history
    val update = msg.body as Update
    if (update.propose.index > hist.size)
      throw new RuntimeException("Trying to update ahead!")
    
    if (update.propose.index === hist.size)
      addHistory(msg)
    else
      setHistory(update.propose.index, msg)
    
    lastIndex  = update.propose.index
  }
  
  def Message getVote()
  def void setVote(Message vote)
  
  def int getLastIndex()
  def void setLastIndex(int index)
  
  def List<Message> getHistory()
  
  protected def void addHistory(Message msg)
  protected def void setHistory(int index, Message msg)
}
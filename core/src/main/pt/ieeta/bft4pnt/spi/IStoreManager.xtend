package pt.ieeta.bft4pnt.spi

import java.util.List
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Update
import pt.ieeta.bft4pnt.msg.Quorum

abstract class IStoreManager {
  public static val quorumAlias = "quorum"
  
  synchronized def Quorum getCurrentQuorum() {
    val qRec = local.getRecord(alias(quorumAlias))
    if (qRec === null)
      throw new RuntimeException('''No quorum record!''')
    
    getQuorumAt(qRec.lastIndex)
  }
  
  synchronized def Quorum getQuorumAt(int index) {
    val qRec = local.getRecord(alias(quorumAlias))
    if (qRec === null)
      throw new RuntimeException('''No quorum record!''')
    
    val qMsg = qRec.getCommit(index)
    if (qMsg === null)
      throw new RuntimeException('''No quorum at index: «index»''')
    
    val q = qMsg.data.get(Quorum)
    if (q.index !== index)
      throw new RuntimeException('''Incorrect quorum index: («q.index» != «index»)''')
    
    return q
  }
  
  def local() { internalGetOrCreate("local") }
  
  def getOrCreate(String udi) {
    if (udi == "local")
      throw new RuntimeException("Reserved store.")
    
    internalGetOrCreate(udi)
  }
  
  def String alias(String alias)
  
  def List<Message> pendingReplicas(int minimum)
  
  protected def IStore internalGetOrCreate(String udi)
}

interface IStore {
  def IRecord insert(Message msg)
  def IRecord getRecord(String record)
}

abstract class IRecord {
  def getType() {
    val insert = history.get(0).body as Insert
    insert.type
  }
  
  def lastCommit() { getCommit(lastIndex) }
  
  def getCommit(int index) {
    if (index === -1) history.get(lastIndex)
    if (index > lastIndex) return null
    
    val msg = history.get(index)
    msg.addReplicaChangeListener[
      if (msg.type === Message.Type.INSERT)
        setHistory(0, msg)
      else
        setHistory((msg.body as Update).propose.index, msg)
    ]
    
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
  
  protected def List<Message> getHistory()
  protected def void addHistory(Message msg)
  protected def void setHistory(int index, Message msg)
}
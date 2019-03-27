package pt.ieeta.bft4pnt.store

import java.util.List
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.QuorumConfig
import pt.ieeta.bft4pnt.msg.Slices

interface IStore {
  def QuorumConfig quorum()
  def void setQuorum(QuorumConfig quorum)
  
  def boolean has(String udi)
  def IClientStore get(String udi)
}

interface IClientStore {
  def void insert(Message msg) // always accept, override existing
  
  def IRecord getRecord(String record)
  def IDataStore getData()
}

interface IRecord {
  def Message getVote()
  def void setVote(Message vote)
  
  def int count(String source, String dk)
  def void clear(String source, String dk)
  
  def int lastCommit()  // index >= 0. 0 if there are no updates
  def Message getCommit(int index)
  def Slices slices() // get slices from the last commit
  
  def Message getInsert()
  
  def void update(Message msg) // always accept, override existing commit. Set last commit index to this update. Remove all updates forward?
  def Message getUpdate(int index)
  def List<Message> getAllUpdates()
}

interface IDataStore {
  enum Status { YES, NO, PENDING }
  
  // use record == dk for inserts
  def Status has(String record, String dk)
  def boolean verify(String record, String dk, Slices slices)
}
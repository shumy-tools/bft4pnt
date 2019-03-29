package pt.ieeta.bft4pnt.store

import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.QuorumConfig
import pt.ieeta.bft4pnt.msg.Slices

interface IStore {
  def QuorumConfig getQuorum()
  def void setQuorum(QuorumConfig quorum)
  
  def IClientStore get(String udi)
  def IClientStore create(String udi)
}

interface IClientStore {
  def void insert(Message msg) // always accept, override existing
  
  def IRecord getRecord(String record)
  def IDataStore getData()
}

interface IRecord {
  def Message getVote()
  def void setVote(Message vote)
  
  def int lastCommit()  // index >= 0. 0 if there are no updates
  def Message getCommit(int index)
  
  def Slices slices() // get slices from the last commit
  
  def void update(Message msg) // always accept, override existing commit. Set last commit index to this update. Remove all updates forward?
}

interface IDataStore {
  enum Status { YES, NO, PENDING }
  
  // use record == dk for inserts
  def Status has(String record, String dk)
  def boolean verify(String record, String dk, Slices slices)
}
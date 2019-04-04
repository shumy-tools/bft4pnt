package pt.ieeta.bft4pnt.spi

import pt.ieeta.bft4pnt.msg.Message

interface IStoreManager {
  def String alias(String alias) // get local record for alias
  
  def IStore local()
  def IStore getOrCreate(String udi)
}

interface IStore {
  def IRecord insert(Message msg)
  def IRecord getRecord(String record)
}

interface IRecord {
  def String getType()
  
  def Message getVote()
  def void setVote(Message vote)
  
  def int lastIndex()  // index >= 0. 0 if there are no updates
  def Message lastCommit()
  def Message getCommit(int index)
  
  def void update(Message msg) // always accept, override existing commit. Set last commit index to this update. Remove all updates forward?
}
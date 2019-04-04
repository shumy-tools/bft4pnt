package pt.ieeta.bft4pnt.spi

import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Update

interface IExtension {
  // validate messages before processing. Return null to continue processing
  def String checkInsert(IStore cs, Message msg, Insert insert)
  def String checkUpdate(IStore cs, Message msg, Update update)
  
  // return true if should continue (insert, update) automatic store.
  // return false if the store process is handled in the extension
  def boolean insert(IStore cs, Message msg)
  def boolean update(IStore cs, Message msg)
}
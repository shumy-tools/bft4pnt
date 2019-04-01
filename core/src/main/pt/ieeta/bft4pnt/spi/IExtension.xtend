package pt.ieeta.bft4pnt.spi

import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose

interface IExtension {
  // validate messages before processing. Return null to continue processing
  def String checkPropose(IClientStore cs, Message msg, Propose propose)
  def String checkInsert(IClientStore cs, Message msg, Insert insert)
  
  // return true if should continue (insert, update) automatic store.
  // return false if the store process is handled in the extension
  def boolean insert(IClientStore cs, Message msg)
  def boolean update(IClientStore cs, Message msg)
}
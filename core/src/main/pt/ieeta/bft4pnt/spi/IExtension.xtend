package pt.ieeta.bft4pnt.spi

import pt.ieeta.bft4pnt.msg.Message

interface IExtension {
  // Validate before processing. Return a message constraint error or null to continue.
  def String onInsert(Store cs, Message msg)
  def String onUpdate(Store cs, Message msg)
}
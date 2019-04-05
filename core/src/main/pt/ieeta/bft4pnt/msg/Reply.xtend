package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Reply implements ISection {
  //WARNING: don't change the position of defined types.
  enum Type { VOTE, NO_DATA, RECEIVING, ACK }
  
  public val Type type
  public val Party party
  
  public val Propose propose
  public val byte[] replica
  
  static def Reply vote(Party party, Propose propose) {
    return new Reply(Type.VOTE, party, propose, null)
  }
  
  static def Reply noData(Party party) {
    return new Reply(Type.NO_DATA, party, null, null)
  }
  
  static def Reply noData(Party party, Propose propose) {
    return new Reply(Type.NO_DATA, party, propose, null)
  }
  
  
  static def Reply receiving(Party party) {
    return new Reply(Type.RECEIVING, party, null, null)
  }
    
  static def Reply receiving(Party party, Propose propose) {
    return new Reply(Type.RECEIVING, party, propose, null)
  }
  
  
  static def Reply ack(Party party, byte[] replica) {
    return new Reply(Type.ACK, party, null, replica)
  }
  
  static def Reply ack(Party party, Propose propose, byte[] replica) {
    return new Reply(Type.ACK, party, propose, replica)
  }
  
  override write(ByteBuf buf) {
    buf.writeShort(type.ordinal)
    party.write(buf)
    
    if (propose !== null) {
      buf.writeBoolean(true)
      propose.write(buf)
    } else
      buf.writeBoolean(false)
    
    Message.writeBytes(buf, replica)
  }
  
  static def Reply read(ByteBuf buf) {
    val typeIndex = buf.readShort as int
    val type = Type.values.get(typeIndex)
    val party = Party.read(buf)
    
    val hasPropose = buf.readBoolean
    val propose = if (hasPropose) Propose.read(buf)
    
    val replica = Message.readBytes(buf)
    
    return new Reply(type, party, propose, replica)
  }
}
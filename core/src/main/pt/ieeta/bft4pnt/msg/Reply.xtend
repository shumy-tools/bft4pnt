package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Reply implements ISection {
  //WARNING: don't change the position of defined types.
  enum Type { VOTE, NO_DATA, RECEIVING, ACK }
  
  public val Type type
  public val QuorumConfig quorum
  public val Propose propose
  
  static def Reply vote(QuorumConfig quorum, Propose propose) {
    return new Reply(Type.VOTE, quorum, propose)
  }
  
  static def Reply noData() {
    return new Reply(Type.NO_DATA, null, null)
  }
  
  static def Reply receiving() {
    return new Reply(Type.RECEIVING, null, null)
  }
  
  static def Reply ack() {
    return new Reply(Type.ACK, null, null)
  }
  
  override write(ByteBuf buf) {
    buf.writeShort(type.ordinal)
    
    if (type === Type.VOTE) {
      quorum.write(buf)
      propose.write(buf)
    }
  }
  
  static def Reply read(ByteBuf buf) {
    val typeIndex = buf.readShort as int
    val type = Type.values.get(typeIndex)
    
    if (type === Type.VOTE) {
      val quorum = QuorumConfig.read(buf)
      val propose = Propose.read(buf)
      return new Reply(type, quorum, propose)
    }
    
    return new Reply(type, null, null)
  }
}
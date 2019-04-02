package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Reply implements ISection {
  //WARNING: don't change the position of defined types.
  enum Type { VOTE, NO_DATA, RECEIVING, ACK }
  
  public val Type type
  public val Integer party
  
  public val QuorumConfig quorum
  public val Propose propose
  
  static def Reply vote(Integer party, QuorumConfig quorum, Propose propose) {
    return new Reply(Type.VOTE, party, quorum, propose)
  }
  
  static def Reply noData(Integer party) {
    return new Reply(Type.NO_DATA, party, null, null)
  }
  
  static def Reply receiving(Integer party) {
    return new Reply(Type.RECEIVING, party, null, null)
  }
  
  static def Reply ack(Integer party) {
    return new Reply(Type.ACK, party, null, null)
  }
  
  override write(ByteBuf buf) {
    buf.writeShort(type.ordinal)
    buf.writeInt(party)
    
    if (type === Type.VOTE) {
      quorum.write(buf)
      propose.write(buf)
    }
  }
  
  static def Reply read(ByteBuf buf) {
    val typeIndex = buf.readShort as int
    val party = buf.readInt
    
    val type = Type.values.get(typeIndex)
    if (type === Type.VOTE) {
      val quorum = QuorumConfig.read(buf)
      val propose = Propose.read(buf)
      return new Reply(type, party, quorum, propose)
    }
    
    return new Reply(type, party, null, null)
  }
}
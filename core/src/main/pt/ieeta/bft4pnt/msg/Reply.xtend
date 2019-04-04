package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Reply implements ISection {
  //WARNING: don't change the position of defined types.
  enum Type { VOTE, NO_DATA, RECEIVING, ACK }
  
  public val Type type
  public val Integer party
  public val Integer quorum
  
  public val Propose propose
  public val String fingerprint
  
  static def Reply vote(Integer party, Integer quorum, Propose propose) {
    return new Reply(Type.VOTE, party, quorum, propose, null)
  }
  
  static def Reply noData(Integer party) {
    return new Reply(Type.NO_DATA, party, null, null, null)
  }
  
  static def Reply noData(Integer party, String fingerprint) {
    return new Reply(Type.NO_DATA, party, null, null, fingerprint)
  }
  
  
  static def Reply receiving(Integer party) {
    return new Reply(Type.RECEIVING, party, null, null, null)
  }
    
  static def Reply receiving(Integer party, String fingerprint) {
    return new Reply(Type.RECEIVING, party, null, null, fingerprint)
  }
  
  
  static def Reply ack(Integer party) {
    return new Reply(Type.ACK, party, null, null, null)
  }
  
  static def Reply ack(Integer party, String fingerprint) {
    return new Reply(Type.ACK, party, null, null, fingerprint)
  }
  
  override write(ByteBuf buf) {
    buf.writeShort(type.ordinal)
    buf.writeInt(party)
    
    if (type === Type.VOTE) {
      buf.writeInt(quorum)
      propose.write(buf)
    } else {
      Message.writeString(buf, fingerprint)
    }
  }
  
  static def Reply read(ByteBuf buf) {
    val typeIndex = buf.readShort as int
    val party = buf.readInt
    
    val type = Type.values.get(typeIndex)
    return if (type === Type.VOTE) {
      val quorum = buf.readInt
      val propose = Propose.read(buf)
      new Reply(type, party, quorum, propose, null)
    } else {
      val fingerprint = Message.readString(buf)
      new Reply(type, party, null, null, fingerprint)
    }
  }
}
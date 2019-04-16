package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import java.security.PublicKey
import pt.ieeta.bft4pnt.crypto.KeyPairHelper

@FinalFieldsConstructor
class Reply implements ISection {
  //WARNING: don't change the position of defined types.
  enum Type { VOTE, NO_DATA, RECEIVING, ACK }
  
  public val Type type
  public val PublicKey party
  
  public val Propose propose
  public val byte[] replica // is the signature of the (insert, update) message
  
  static def Reply vote(PublicKey party, Propose propose) {
    return new Reply(Type.VOTE, party, propose, null)
  }
  
  static def Reply noData(PublicKey party) {
    return new Reply(Type.NO_DATA, party, null, null)
  }
  
  static def Reply noData(PublicKey party, Propose propose) {
    return new Reply(Type.NO_DATA, party, propose, null)
  }
  
  
  static def Reply receiving(PublicKey party) {
    return new Reply(Type.RECEIVING, party, null, null)
  }
    
  static def Reply receiving(PublicKey party, Propose propose) {
    return new Reply(Type.RECEIVING, party, propose, null)
  }
  
  
  static def Reply ack(PublicKey party, byte[] replica) {
    return new Reply(Type.ACK, party, null, replica)
  }
  
  static def Reply ack(PublicKey party, Propose propose, byte[] replica) {
    return new Reply(Type.ACK, party, propose, replica)
  }
  
  def String strParty() {
    KeyPairHelper.encode(party)
  }
  
  override write(ByteBuf buf) {
    buf.writeShort(type.ordinal)
    Message.writeBytes(buf, party.encoded)
    
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
    val party = Message.readBytes(buf)
    
    val hasPropose = buf.readBoolean
    val propose = if (hasPropose) Propose.read(buf)
    
    val replica = Message.readBytes(buf)
    
    return new Reply(type, KeyPairHelper.read(party), propose, replica)
  }
}
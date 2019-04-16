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
  public val Integer quorum
  public val PublicKey party
  
  public val Propose propose
  public val byte[] replica // is the signature of the (insert, update) message
  
  static def Reply vote(Integer quorum, PublicKey party, Propose propose) {
    return new Reply(Type.VOTE, quorum, party, propose, null)
  }
  
  static def Reply noData(Integer quorum, PublicKey party) {
    return new Reply(Type.NO_DATA, quorum, party, null, null)
  }
  
  static def Reply noData(Integer quorum, PublicKey party, Propose propose) {
    return new Reply(Type.NO_DATA, quorum, party, propose, null)
  }
  
  
  static def Reply receiving(Integer quorum, PublicKey party) {
    return new Reply(Type.RECEIVING, quorum, party, null, null)
  }
    
  static def Reply receiving(Integer quorum, PublicKey party, Propose propose) {
    return new Reply(Type.RECEIVING, quorum, party, propose, null)
  }
  
  
  static def Reply ack(Integer quorum, PublicKey party, byte[] replica) {
    return new Reply(Type.ACK, quorum, party, null, replica)
  }
  
  static def Reply ack(Integer quorum, PublicKey party, Propose propose, byte[] replica) {
    return new Reply(Type.ACK, quorum, party, propose, replica)
  }
  
  def String strParty() {
    KeyPairHelper.encode(party)
  }
  
  override write(ByteBuf buf) {
    buf.writeShort(type.ordinal)
    buf.writeInt(quorum)
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
    val quorum = buf.readInt
    val party = Message.readBytes(buf)
    
    val hasPropose = buf.readBoolean
    val propose = if (hasPropose) Propose.read(buf)
    
    val replica = Message.readBytes(buf)
    
    return new Reply(type, quorum, KeyPairHelper.read(party), propose, replica)
  }
}
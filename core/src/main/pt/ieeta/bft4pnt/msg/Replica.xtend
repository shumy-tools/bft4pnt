package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.security.PublicKey
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.ArraySlice
import pt.ieeta.bft4pnt.crypto.SignatureHelper

@FinalFieldsConstructor
class Replica implements ISection {
  public val ArraySlice slice
  
  public val Party party
  public val byte[] signature
  
  def boolean verifySignature(PublicKey key) {
    SignatureHelper.verify(key, slice, signature)
  }
  
  override write(ByteBuf buf) {
    party.write(buf)
    Message.writeBytes(buf, signature)
  }
  
  static def Replica read(ByteBuf buf, ArraySlice slice) {
    val party = Party.read(buf)
    val signature = Message.readBytes(buf)
    
    return new Replica(slice, party, signature)
  }
} 
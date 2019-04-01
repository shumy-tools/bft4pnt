package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.security.PublicKey
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.crypto.SignatureHelper
import pt.ieeta.bft4pnt.crypto.ArraySlice

@FinalFieldsConstructor
class Signature implements ISection {
  public val PublicKey source
  public val byte[] signature
  
  def verify (ArraySlice slice) {
    SignatureHelper.verify(source, slice, signature)
  }
  
  override write(ByteBuf buf) {
    Message.writeBytes(buf, source.encoded)
    Message.writeBytes(buf, signature)
  }
  
  static def Signature read(ByteBuf buf) {
    val key = Message.readBytes(buf)
    val signature = Message.readBytes(buf)
    
    val source = KeyPairHelper.read(key)
    return new Signature(source, signature)
  }
}
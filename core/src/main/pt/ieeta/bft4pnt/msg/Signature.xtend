package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.security.PublicKey
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.crypto.SignatureHelper
import pt.ieeta.bft4pnt.crypto.ArraySlice
import pt.ieeta.bft4pnt.crypto.DigestHelper

@FinalFieldsConstructor
class Signature implements ISection {
  public val PublicKey source
  public val byte[] signature
  
  def String strSource() {
    KeyPairHelper.encode(source)
  }
  
  def verify(ArraySlice slice) {
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
  
  override equals(Object obj) {
    if (obj === null)
      return false
    
    if (obj === this)
      return true
    
    if (obj instanceof Signature)
      return DigestHelper.digest(this.source.encoded) == DigestHelper.digest(obj.source.encoded)
        && DigestHelper.digest(this.signature) == DigestHelper.digest(obj.signature)
    
    return false
  }
}
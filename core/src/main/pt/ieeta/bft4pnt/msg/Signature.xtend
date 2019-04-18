package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.security.PublicKey
import pt.ieeta.bft4pnt.crypto.ArraySlice
import pt.ieeta.bft4pnt.crypto.DigestHelper
import pt.ieeta.bft4pnt.crypto.CryptoHelper

//@FinalFieldsConstructor
class Signature implements ISection {
  public val PublicKey source
  public val byte[] signature
  
  public val String strSource
  
  new (PublicKey source, byte[] signature) {
    this.source = source
    this.signature = signature
    this.strSource = CryptoHelper.encode(source)
  }
  
  def verify(ArraySlice slice) {
    val verifier = java.security.Signature.getInstance(CryptoHelper.SIG_ALG, CryptoHelper.PROVIDER) => [
      initVerify(source)
      update(slice.data, slice.offset, slice.length)
    ]
    
    return verifier.verify(signature)
  }
  
  override write(ByteBuf buf) {
    Message.writeBytes(buf, source.encoded)
    Message.writeBytes(buf, signature)
  }
  
  static def Signature read(ByteBuf buf) {
    val key = Message.readBytes(buf)
    val signature = Message.readBytes(buf)
    
    val source = CryptoHelper.read(key)
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
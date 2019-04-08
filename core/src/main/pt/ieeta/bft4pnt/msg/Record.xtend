package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Record implements ISection {
  public val String udi
  public val String fingerprint
  
  override write(ByteBuf buf) {
    Message.writeString(buf, udi)
    Message.writeString(buf, fingerprint)
  }
  
  static def Record read(ByteBuf buf) {
    val udi = Message.readString(buf)
    val fingerprint = Message.readString(buf)
    
    return new Record(udi, fingerprint)
  }
  
  override equals(Object obj) {
    if (obj === this)
      return true
    
    if (obj instanceof Record)
      return this.udi == obj.udi && this.fingerprint == obj.fingerprint
    
    return false
  }
  
  override hashCode() {
    udi.hashCode + fingerprint.hashCode
  }
}
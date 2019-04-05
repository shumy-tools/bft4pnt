package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Party implements ISection {
  public val Integer index
  public val Integer quorum
  
  override write(ByteBuf buf) {
    buf.writeInt(index)
    buf.writeInt(quorum)
  }
  
  static def Party read(ByteBuf buf) {
    val index = buf.readInt
    val quorum = buf.readInt
    
    return new Party(index, quorum)
  }
  
  override equals(Object obj) {
    if (obj === this)
      return true
    
    if (obj instanceof Party)
      return this.index === obj.index && this.quorum === obj.quorum
    
    return false
  }
  
  override hashCode() {
    index.hashCode + quorum.hashCode
  }
  
}
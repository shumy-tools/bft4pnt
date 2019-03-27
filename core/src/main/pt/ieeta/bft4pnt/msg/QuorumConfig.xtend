package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class QuorumConfig implements ISection {
  public val int n
  public val int t
  
  override write(ByteBuf buf) {
    buf.writeInt(n)
    buf.writeInt(t)
  }
  
  static def QuorumConfig read(ByteBuf buf) {
    val n = buf.readInt
    val t = buf.readInt
    
    return new QuorumConfig(n, t)
  }
}
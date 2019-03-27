package pt.ieeta.bft4pnt.msg

import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import io.netty.buffer.ByteBuf

@FinalFieldsConstructor
class Propose implements ISection {
  public val int index
  public val String fingerprint
  public val long round
  
  override write(ByteBuf buf) {
    buf.writeInt(index)
    Message.writeString(buf, fingerprint)
    buf.writeLong(round)
  }
  
  static def Propose read(ByteBuf buf) {
    val index = buf.readInt
    val fingerprint = Message.readString(buf)
    val round = buf.readLong
    
    return new Propose(index, fingerprint, round)
  }
}
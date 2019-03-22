package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Insert implements ISection {
  public val String type
  public val Slices slices
  
  new(String type) {
    this.type = type
    this.slices = new Slices
  }
  
  override write(ByteBuf buf) {
    Message.writeString(buf, type)
    slices.write(buf)
  }
  
  static def Insert read(ByteBuf buf) {
    val type = Message.readString(buf)
    val slices = Slices.read(buf)
    
    return new Insert(type, slices)
  }
}
package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.eclipse.xtend.lib.annotations.Accessors

@FinalFieldsConstructor
class Insert implements ISection, HasSlices {
  public val String type
  @Accessors val Slices slices
  
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
  
  static def Message create(long msgId, String udi, String type, Data block) {
    val record = new Record(udi, block.fingerprint)
    val body = new Insert(type)
  
    return new Message(record, body) => [
      id = msgId
      data = block
    ]
  }
}
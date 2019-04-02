package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import java.nio.charset.StandardCharsets
import pt.ieeta.bft4pnt.crypto.HashHelper

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
  
  static def Message create(long msgId, String udi, String type, String data) {
    val block = data.getBytes(StandardCharsets.UTF_8)
    val record = new Record(udi, HashHelper.digest(block))
    val body = new Insert(type)
    
    return new Message(record, body) => [
      id = msgId
      data = block
    ]
  }
}
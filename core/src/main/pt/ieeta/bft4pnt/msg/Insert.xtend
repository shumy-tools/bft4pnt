package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.nio.charset.StandardCharsets
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
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
  
  static def Message create(long msgId, String udi, String type, ISection section) {
    val buf = PooledByteBufAllocator.DEFAULT.buffer(1024)
    try {
      buf.retain
      section.write(buf)
      
      val block = newByteArrayOfSize(buf.readableBytes)
      buf.getBytes(buf.readerIndex, block)
      buf.release
      
      return create(udi, type, HashHelper.digest(block)) => [
        id = msgId
        data = new Data(Data.Type.SECTION, block)
      ]
    } finally {
      buf.release
    }
  }
  
  static def Message create(long msgId, String udi, String type, String data) {
    val block = data.getBytes(StandardCharsets.UTF_8)
    return create(udi, type, HashHelper.digest(block)) => [
      id = msgId
      data = new Data(Data.Type.STRING, block)
    ]
  }
  
  private static def create(String udi, String type, String rec) {
    val record = new Record(udi, rec)
    val body = new Insert(type)
    return new Message(record, body)
  }
}
package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class GetRetrieve implements ISection {
  public val long size
  public val String record
  public val int index
  
  public val String slice
  
  override write(ByteBuf buf) {
    buf.writeLong(size)
    Message.writeString(buf, record)
    buf.writeInt(index)
    
    Message.writeString(buf, slice)
  }
  
  static def GetRetrieve read(ByteBuf buf) {
    val size = buf.readLong
    val record = Message.readString(buf)
    val index = buf.readInt
    
    val slice = Message.readString(buf)
    
    return new GetRetrieve(size, record, index, slice)
  }
}
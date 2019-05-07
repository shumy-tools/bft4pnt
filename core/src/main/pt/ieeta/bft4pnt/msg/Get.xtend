package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Get implements ISection {
  public val int index  // -1 is a request for the most up-to-date commit
  public val int slice  // -1 indicates the whole data block
  
  override write(ByteBuf buf) {
    buf.writeInt(index)
    buf.writeInt(slice)
  }
  
  static def Get read(ByteBuf buf) {
    val index = buf.readInt
    val slice = buf.readInt
    
    return new Get(index, slice)
  }
  
  static def Message create(long msgId, String udi, String fingerprint) {
    create(msgId, udi, fingerprint, -1, -1)
  }
  
  static def Message create(long msgId, String udi, String fingerprint, int index, int slice) {
    val record = new Record(udi, fingerprint)
    val body = new Get(index, slice)
  
    return new Message(record, body) => [
      id = msgId
    ]
  }
}
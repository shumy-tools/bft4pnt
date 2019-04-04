package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

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
  
  static def Message create(long msgId, String udi, String rec, String fingerprint, int index, long round) {
    val record = new Record(udi, rec)
    val body = new Propose(index, fingerprint, round)
  
    return new Message(record, body) => [
      id = msgId
    ]
  }
}
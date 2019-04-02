package pt.ieeta.bft4pnt.msg

import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets
import pt.ieeta.bft4pnt.crypto.HashHelper

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
  
  static def Message create(long msgId, String udi, String rec, String data, int index, long round) {
    val block = data.getBytes(StandardCharsets.UTF_8)
    val record = new Record(udi, rec)
    val body = new Propose(index, HashHelper.digest(block), round)
    
    return new Message(record, body) => [
      id = msgId
      data = block
    ]
  }
}
package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.nio.charset.StandardCharsets
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.HashHelper

@FinalFieldsConstructor
class Propose implements ISection {
  public val int index
  public val String fingerprint
  public val long round
  
  // cache data is transfered to Update message
  package var Data.Type type
  package var byte[] data
  
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
  
  static def Message create(long msgId, String udi, String rec, ISection section, int index, long round) {
    val buf = PooledByteBufAllocator.DEFAULT.buffer(1024)
    try {
      buf.retain
      section.write(buf)
      
      val block = newByteArrayOfSize(buf.readableBytes)
      buf.getBytes(buf.readerIndex, block)
      buf.release
      
      return create(udi, rec, HashHelper.digest(block), Data.Type.SECTION, block, index, round) => [
        id = msgId
      ]
    } finally {
      buf.release
    }
  }
  
  static def Message create(long msgId, String udi, String rec, String data, int index, long round) {
    val block = data.getBytes(StandardCharsets.UTF_8)
    return create(udi, rec, HashHelper.digest(block), Data.Type.STRING, block, index, round) => [
      id = msgId
    ]
  }
  
  private static def create(String udi, String rec, String dk, Data.Type type, byte[] data, int index, long round) {
    val record = new Record(udi, rec)
    val body = new Propose(index, dk, round)
    body.type = type
    body.data = data
    
    return new Message(record, body)
  }
}
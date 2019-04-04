package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import org.bouncycastle.util.encoders.Base64

class Data implements ISection {
  enum Type { RAW, SECTION, STRING }
  enum Status { YES, NO, PENDING }
  
  public val Type type
  public val int size
  
  //TODO: change method for big data!
  var byte[] raw
  
  new(byte[] raw) { this(Type.RAW, raw) }
  new(Type type, byte[] raw) {
    this.type = type
    this.size = raw.length
    this.raw = raw
  }
  
  def has(String key) {
    //TODO: change method for big data!
    return Status.YES
  }
  
  def byte[] getRaw() { raw }
  
  def getString() {
    if (type !== Type.STRING)
      throw new RuntimeException('''Wrong data type retrieve! (type=«type», try=«Type.STRING»)''')
    
    return new String(raw, StandardCharsets.UTF_8)
  }
  
  def <T extends ISection> T get((ByteBuf)=>T reader) {
    if (type !== Type.SECTION)
      throw new RuntimeException('''Wrong data type retrieve! (type=«type», try=«Type.SECTION»)''')
    
    val buf = PooledByteBufAllocator.DEFAULT.buffer(1024)
    try {
      buf.retain
      buf.writeBytes(raw)
      return reader.apply(buf)
    } finally {
      buf.release
    }
  }
  
  def verify(String key, Slices slices) {
    val digest = MessageDigest.getInstance("SHA-256")
    val dRes = digest.digest(raw)
    val fingerprint = new String(Base64.encode(dRes), StandardCharsets.UTF_8)
    
    return key == fingerprint
    
    //TODO: verify slices?
  }
  
  override write(ByteBuf buf) {
    //TODO: change method for big data!
    buf.writeShort(type.ordinal)
    buf.writeInt(size)
    buf.writeBytes(raw)
  }
  
  static def Data read(ByteBuf buf) {
    //TODO: change method for big data!
    val typeIndex = buf.readShort as int
    
    val type = Type.values.get(typeIndex)
    val size = buf.readInt
    
    val raw = newByteArrayOfSize(size)
    buf.readBytes(raw)
    
    return new Data(type, raw)
  }
}
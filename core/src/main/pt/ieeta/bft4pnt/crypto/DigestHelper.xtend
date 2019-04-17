package pt.ieeta.bft4pnt.crypto

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import org.bouncycastle.util.encoders.Base64
import pt.ieeta.bft4pnt.msg.ISection

class DigestHelper {
  
  static def String digest(byte[] value) {
    val inst = MessageDigest.getInstance("SHA-256")
    return new String(Base64.encode(inst.digest(value)), StandardCharsets.UTF_8)
  }
  
  static def String digest(ByteBuf value) {
    try {
      value.retain
      val length = value.readableBytes
      val data = newByteArrayOfSize(length)
      value.getBytes(value.readerIndex, data)
      return digest(data)
    } finally {
      value.release
    }
  }
  
  static def String digest(String value) {
    return digest(value.getBytes(StandardCharsets.UTF_8))
  }
  
  static def String digest(ISection value) {
    val buf = PooledByteBufAllocator.DEFAULT.buffer(1024)
    try {
      value.write(buf)
      
      val raw = newByteArrayOfSize(buf.readableBytes)
      buf.readBytes(raw)
      return digest(raw)
    } finally {
      buf.release
    }
  }
}
package pt.ieeta.bft4pnt.crypto

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import org.bouncycastle.util.encoders.Base64
import pt.ieeta.bft4pnt.msg.ISection
import java.io.File
import java.io.FileInputStream
import java.io.RandomAccessFile
import pt.ieeta.bft4pnt.msg.Slices
import java.util.ArrayList

class DigestHelper {
  
  static def Slices digestSlices(File value, int nSlices) {
    val inst = MessageDigest.getInstance("SHA-256")
    
    val raf = new RandomAccessFile(value, "r")
    val sSize = (raf.length / nSlices) as int
    
    val byteArray = newByteArrayOfSize(sSize)
    var bytesCount = 0
    
    val slices = new ArrayList<String>
    val fis = new FileInputStream(value)
    try {
      while ((bytesCount = fis.read(byteArray)) !== -1) {
        inst.reset
        inst.update(byteArray, 0, bytesCount)
        val finger = new String(Base64.encode(inst.digest), StandardCharsets.UTF_8)
        slices.add(finger)
      }
    } finally {
      fis.close
    }
    
    return new Slices(sSize, slices)
  }
  
  static def String digest(File value) {
    val inst = MessageDigest.getInstance("SHA-256")
    
    val byteArray = newByteArrayOfSize(1024)
    var bytesCount = 0
    
    val fis = new FileInputStream(value)
    try {
      while ((bytesCount = fis.read(byteArray)) !== -1) {
        inst.update(byteArray, 0, bytesCount)
      }
    } finally {
      fis.close
    }
    
    return new String(Base64.encode(inst.digest), StandardCharsets.UTF_8)
  }
  
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
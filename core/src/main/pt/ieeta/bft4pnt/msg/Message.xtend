package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.nio.charset.StandardCharsets
import java.security.PrivateKey
import java.security.PublicKey
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.ArraySlice
import pt.ieeta.bft4pnt.crypto.SignatureHelper

@FinalFieldsConstructor
class Message {
  //WARNING: don't change the position of defined types.
  enum Type {
    INSERT, UPDATE, PROPOSE, VOTE,
    CONSULT, RETRIVE,
    ERROR
  }
  
  public val int version
  public val Type type
  public val String ext
  
  public val Record record
  public val ISection body
  
  new(Record record, ISection body) { this(null, record, body) }
  new(String ext, Record record, ISection body) {
    this.version = 1
    this.type = switch body {
      Insert: Type.INSERT
      Update: Type.UPDATE
      Propose: Type.PROPOSE
      Consult: Type.CONSULT
      Retrieve: Type.RETRIVE
      Error: Type.ERROR
    }
    
    this.ext = null
    this.record = record
    this.body = body
  }
  
  def ByteBuf write(PrivateKey prvKey) {
    PooledByteBufAllocator.DEFAULT.buffer(1024) => [
      writeShort(version)
      writeShort(type.ordinal)
      writeString(ext)
      
      record.write(it)
      body.write(it)
      
      //sign the message
      val signature = SignatureHelper.sign(prvKey, getDataBlock)
      writeInt(signature.length)
      writeBytes(signature)
    ]
  } 
  
  static def ReadResult read(ByteBuf buf, PublicKey pubKey) {
    buf.retain
    try {
      val data = buf.getDataBlock
      
      val version = buf.readShort as int
      if (version > 1)
        return new ReadResult("Non supported version!")
      
      val typeIndex = buf.readShort as int
      val ext = buf.readString
      
      val record = Record.read(buf)
      
      val type = Type.values.get(typeIndex)
      val ISection body = switch type {
        case INSERT: Insert.read(buf)
        case UPDATE: Update.read(buf)
        case PROPOSE: Propose.read(buf)
        case VOTE: Vote.read(buf)
        
        case CONSULT: Consult.read(buf)
        case RETRIVE: Retrieve.read(buf)
        
        case ERROR: Error.read(buf)
        default: return new ReadResult("Unrecognized message type!")
      }
      
      val signSize = buf.readInt
      val signature = if (signSize > 0) {
        val signBuf = newByteArrayOfSize(signSize)
        buf.readBytes(signBuf)
        signBuf
      }
      
      //validate signature
      if (!SignatureHelper.verify(pubKey, data, signature))
        return new ReadResult("Incorrect signature!")
      
      val msg = new Message(version, type, ext, record, body)
      return new ReadResult(msg)
    } catch (Throwable ex) {
      return new ReadResult(ex.message)
    } finally {
      buf.release
    }
  }
  
  private static def ArraySlice getDataBlock(ByteBuf buf) {
    var byte[] data = null
    var int offset = 0
    val length = buf.readableBytes
    
    if (buf.hasArray) {
      data = buf.array
      offset = buf.arrayOffset
    } else {
      data = newByteArrayOfSize(length)
      buf.getBytes(buf.readerIndex, data)
      offset = 0
    }
    
    return new ArraySlice(data, offset, length)
  }
  
  package static def void writeString(ByteBuf buf, String value) {
    if (value === null || value.length === 0) {
      buf.writeInt(0)
      return;
    }
    
    buf.writeInt(value.length)
    buf.writeBytes(value.getBytes(StandardCharsets.UTF_8))
  }
  
  package static def String readString(ByteBuf buf) {
    val size = buf.readInt
    if (size === 0) return null
    
    val sBuf = newByteArrayOfSize(size)
    buf.readBytes(sBuf)
    return new String(sBuf, StandardCharsets.UTF_8)
  }
}
package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.security.PrivateKey
import java.security.PublicKey
import pt.ieeta.bft4pnt.crypto.ArraySlice
import pt.ieeta.bft4pnt.crypto.SignatureHelper

class Message implements ISection {
  //WARNING: don't change the position of defined types.
  enum Type {
    INSERT, UPDATE, PROPOSE, VOTE, GET, ERROR
  }
  
  public var long id = 0
  public var InetSocketAddress address = null
  public var String source = null
  byte[] signature = null
  
  public val int version
  public val Type type
  public val Record record
  public val ISection body
  
  new(Record record, ISection body) {
    this.version = 1
    this.type = switch body {
      Insert: Type.INSERT
      Update: Type.UPDATE
      Propose: Type.PROPOSE
      Get: Type.GET
      Error: Type.ERROR
    }
    
    this.record = record
    this.body = body
  }
  
  new(int version, Type type, Record record, ISection body) {
    this.version = version
    this.type = type
    
    this.record = record
    this.body = body
  }
  
  override write(ByteBuf buf) {
    //TODO: write data cache?
    
    buf.writeLong(id) // id is not part of the message signature
    buf.writeShort(version)
    buf.writeShort(type.ordinal)
    
    record.write(buf)
    body.write(buf)
    
    if (signature !== null)
      writeBytes(buf, signature)
  }
  
  def ByteBuf write(PrivateKey prvKey) {
    PooledByteBufAllocator.DEFAULT.buffer(1024) => [
      this.write(it)
      
      //sign the message
      this.signature = SignatureHelper.sign(prvKey, signedBlock)
      writeBytes(it, signature)
    ]
  }
  
  def ByteBuf write() {
    PooledByteBufAllocator.DEFAULT.buffer(1024) => [
      this.write(it)
    ]
  }
  
  static def ReadResult read(ByteBuf buf, PublicKey pubKey) {
    buf.retain
    try {
      val data = buf.signedBlock
      val id = buf.readLong // id is not part of the message signature
      
      val version = buf.readShort as int
      if (version > 1)
        return new ReadResult("Non supported version!")
      
      val typeIndex = buf.readShort as int
      
      val record = Record.read(buf)
      
      val type = Type.values.get(typeIndex)
      val ISection body = switch type {
        case INSERT: Insert.read(buf)
        case UPDATE: Update.read(buf)
        case PROPOSE: Propose.read(buf)
        case VOTE: Reply.read(buf)
        case GET: Get.read(buf)
        
        case ERROR: Error.read(buf)
        default: return new ReadResult("Unrecognized message type!")
      }
      
      // Parties should always verify the correctness of digital signatures before accepting any messages
      val signature = readBytes(buf)
      if (!SignatureHelper.verify(pubKey, data, signature))
        return new ReadResult("Incorrect signature!")
      
      val msg = new Message(version, type, record, body)
      msg.id = id
      msg.signature = signature
      
      return new ReadResult(msg)
    } catch (Throwable ex) {
      ex.printStackTrace
      return new ReadResult(ex.message)
    } finally {
      buf.release
    }
  }
  
  static def ArraySlice getSignedBlock(ByteBuf buf) {
    var byte[] data = null
    var int offset = 8 // id is not part of the message signature
    val length = buf.readableBytes
    
    if (buf.hasArray) {
      data = buf.array
      offset += buf.arrayOffset
    } else {
      data = newByteArrayOfSize(length)
      buf.getBytes(buf.readerIndex, data)
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
  
  package static def void writeBytes(ByteBuf buf, byte[] value) {
    if (value === null || value.length === 0) {
      buf.writeInt(0)
      return;
    }
    
    buf.writeInt(value.length)
    buf.writeBytes(value)
  }
  
  package static def byte[] readBytes(ByteBuf buf) {
    val size = buf.readInt
    if (size === 0) return null
    
    val sBuf = newByteArrayOfSize(size)
    buf.readBytes(sBuf)
    return sBuf
  }
}
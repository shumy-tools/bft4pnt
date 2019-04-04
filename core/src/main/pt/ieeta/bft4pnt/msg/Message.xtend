package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.nio.charset.StandardCharsets
import java.security.KeyPair
import java.security.PrivateKey
import java.util.HashMap
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.ArraySlice
import pt.ieeta.bft4pnt.crypto.SignatureHelper
import org.eclipse.xtend.lib.annotations.Accessors

@FinalFieldsConstructor
class Replica implements ISection {
  public val ArraySlice slice
  public val int party
  public val byte[] signature
  
  override write(ByteBuf buf) {
    buf.writeInt(party)
    Message.writeBytes(buf, signature)
  }
  
  static def Replica read(ByteBuf buf, ArraySlice slice) {
    val party = buf.readInt
    val signature = Message.readBytes(buf)
    
    return new Replica(slice, party, signature)
  }
} 

class Message {
  //WARNING: don't change the position of defined types.
  enum Type {
    INSERT, UPDATE, PROPOSE, REPLY, GET, ERROR
  }
  
  public var long id = 0
  
  public val int version
  public val Type type
  public val Record record
  public val ISection body
  
  var Signature signature
  val replicas = new HashMap<Integer, Replica>
  val rParties = new HashMap<Integer, PrivateKey>
  
  // data for (insert, update)
  @Accessors(PUBLIC_GETTER) var Data data = null
  
  def getSource() { signature.source }
  def getSignature() { signature.signature }
  def getReplicaParties() { replicas.keySet }
  
  def setData(Data data) {
    if (type !== Type.INSERT && type !== Type.UPDATE)
      throw new RuntimeException("Only (insert, update) messages have data!")
    
    this.data = data
  }
  
  def verifyReplicas(Quorum quorum) {
    for (party : replicas.keySet) {
      val key = quorum.getPartyKey(party)
      val replica = replicas.get(party)
      if (!SignatureHelper.verify(key, replica.slice, replica.signature))
        return false
    }
    
    return true
  } 
  
  new(Record record, ISection body) {
    this.version = 1
    this.type = switch body {
      Insert: Type.INSERT
      Update: Type.UPDATE
      Propose: Type.PROPOSE
      Reply: Type.REPLY
      
      Get: Type.GET
      Error: Type.ERROR
      default: throw new RuntimeException("Unrecognized body type: " + body.class)
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
  
  def setReplica(int party, PrivateKey key) {
    rParties.put(party, key)
  }
  
  private def void write(ByteBuf buf) {
    buf.writeLong(id) // id is not part of the message signature
    buf.writeShort(version)
    buf.writeShort(type.ordinal)
    
    record.write(buf)
    body.write(buf)
  }
  
  def ByteBuf write(KeyPair keys) {
    val buf = PooledByteBufAllocator.DEFAULT.buffer(1024)
    try {
      buf.retain
      write(buf)
      val block = buf.signedBlock
      
      signature = new Signature(keys.public, SignatureHelper.sign(keys.private, block))
      signature.write(buf)
      
      if (type === Type.UPDATE) {
        buf.writeInt(replicas.size)
        for (party : rParties.keySet) {
          val rSig = SignatureHelper.sign(rParties.get(party), block)
          val replica = new Replica(block, party, rSig)
          replicas.put(party, replica)
          replica.write(buf)
        }
      }
      
      if (type === Type.INSERT || type === Type.UPDATE)
        data.write(buf)
      
      return buf
    } finally {
      buf.release
    }
  }
  
  def ByteBuf write() {
    val buf = PooledByteBufAllocator.DEFAULT.buffer(1024)
    try {
      buf.retain
      write(buf)
      return buf
    } finally {
      buf.release
    }
  }
  
  static def ReadResult read(ByteBuf buf) {
    buf.retain
    try {
      val block = buf.signedBlock
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
        case REPLY: Reply.read(buf)
        
        case GET: Get.read(buf)
        case ERROR: Error.read(buf)
        default: return new ReadResult("Unrecognized message type!")
      }
      
      // block signature ends here. Count the remaining bytes to remove.
      var less = 0
      
      val signature = Signature.read(buf)
      less += 8 + signature.source.encoded.length + signature.signature.length
      
      val replicas = new HashMap<Integer, Replica>
      if (type === Type.UPDATE) {
        val number = buf.readInt
        less += 4
        for (n : 0 ..< number) {
          val rep = Replica.read(buf, block)
          less += 4 + rep.signature.length
          replicas.put(rep.party, rep)
        }
      }
      
      val data = if (type === Type.INSERT || type === Type.UPDATE) {
        val data = Data.read(buf)
        less += 6 + data.size
        data
      }
      
      // Verify the correctness of the source digital signature
      if (!signature.verify(block.remove(less)))
        return new ReadResult("Incorrect signature!")
      
      val msg = new Message(version, type, record, body)
      msg.id = id
      msg.signature = signature
      msg.replicas.putAll(replicas)
      msg.data = data
      
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
    
    return new ArraySlice(data, offset, length - offset)
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
  
  override toString() '''(id=«id», type=«type», udi=«record.udi», rec=«record.fingerprint»«printType»)'''
  
  private def printType() {
    switch body {
      Insert:   ''', type=«body.type»'''
      Update:   ''', q=«body.quorum» index=«body.propose.index», f=«body.propose.fingerprint», round=«body.propose.round», votes=«body.votes.size»'''
      Propose:  ''', index=«body.index», f=«body.fingerprint», round=«body.round»'''
      Reply:    ''', type=«body.type»«IF body.quorum !== null», q=«body.quorum»«ENDIF»«IF body.propose !== null», index=«body.propose.index», f=«body.propose.fingerprint», round=«body.propose.round»«ENDIF»'''
      Get:      ''', index=«body.index», slices=«body.slices»'''
      Error:    ''', code=«body.code», error=«body.msg»'''
    }
  }
}
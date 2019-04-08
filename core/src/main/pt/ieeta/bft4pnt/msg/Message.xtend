package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.nio.charset.StandardCharsets
import java.security.KeyPair
import java.security.PrivateKey
import java.util.ArrayList
import java.util.Collections
import java.util.List
import org.eclipse.xtend.lib.annotations.Accessors
import pt.ieeta.bft4pnt.crypto.ArraySlice
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.crypto.SignatureHelper
import pt.ieeta.bft4pnt.spi.IStoreManager
import java.util.HashSet
import java.util.HashMap

class Message {
  //WARNING: don't change the position of defined types.
  enum Type {
    INSERT, UPDATE, PROPOSE, REPLY, GET, ERROR
  }
  
  // slice view of the signed data, this data is not transmitted
  @Accessors(PUBLIC_GETTER) var ArraySlice sigSlice = null
  
  public var long id = 0
  public val int version
  public val Type type
  public val Record record
  public val ISection body
  
  var Signature signature
  
  var Data data = new Data
  var List<Replica> replicas = Collections.EMPTY_LIST
  val onReplicasChange = new HashMap<String, ()=>void>
  
  new(Record record, ISection body) { this(1, record, body) }
  new(int version, Record record, ISection body) {
    this.version = version
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
  
  synchronized def getSource() { KeyPairHelper.encode(signature.source) }
  synchronized def getSignature() { signature.signature }
  
  synchronized def Data getData() { data }
  synchronized def void setData(Data data) {
    if (type !== Type.INSERT && type !== Type.UPDATE)
      throw new RuntimeException("Only (insert, update) messages have data!")
    
    this.data = data
  }
  
  def List<Replica> getReplicas() {
    synchronized(onReplicasChange) {
      Collections.unmodifiableList(replicas)
    }
  }
  
  def Replica setLocalReplica(Party party, PrivateKey key) {
    val replicaSig = SignatureHelper.sign(key, sigSlice)
    val rep = new Replica(sigSlice, party, replicaSig)
    
    addReplica(rep)
    return rep
  }
  
  def void addReplica(Replica rep) {
    if (type !== Type.INSERT && type !== Type.UPDATE)
      throw new RuntimeException("Only (insert, update) messages have replicas!")
    
    synchronized(onReplicasChange) {
      //TODO: override existing replica (party, quorum) instead of ignoring?
      if (!replicas.exists[party == rep.party]) {
        this.replicas.add(rep)
        
        //report change to storage
        onReplicasChange.values.forEach[apply]
      }
    }
  }
  
  // count distinct replicas that are part of the current quorum
  def int countReplicas(Party ignore, IStoreManager mng) {
    val q = mng.currentQuorum
    
    // count distinct replicas
    val counts = new HashSet<String>
    synchronized(onReplicasChange) {
      for (rep : replicas) {
        val pQuorum = mng.getQuorumAt(rep.party.quorum)
        val key = pQuorum.getPartyKey(rep.party.index)
        
        val encodedKey = KeyPairHelper.encode(key)
        if (rep.party != ignore && q.contains(encodedKey) && rep.verifySignature(key))
          counts.add(encodedKey)
      }
    }
    
    return counts.size
  }
  
  def int countReplicas(IStoreManager mng) {
    countReplicas(null, mng)
  }
  
  def void addReplicaChangeListener(String name, ()=>void listener) {
    synchronized(onReplicasChange) {
      // avoid multiples listeners for the same source.
      if (!onReplicasChange.containsKey(name))
        onReplicasChange.put(name, listener)
    }
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
      
      if (type === Type.INSERT || type === Type.UPDATE) {
        data.write(buf)
        
        buf.writeInt(replicas.size)
        for (rep : replicas)
          rep.write(buf)
      }
      
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
      
      val b1 = buf.readableBytes
        val signature = Signature.read(buf)
      less += (b1 - buf.readableBytes)
      
      var Data data = null
      val replicas = new ArrayList<Replica>
      if (type === Type.INSERT || type === Type.UPDATE) {
        val b2 = buf.readableBytes
          data = Data.read(buf)
        less += (b2 - buf.readableBytes)
        
        val number = buf.readInt; less += 4
        for (n : 0 ..< number) {
          val b3 = buf.readableBytes
            val rep = Replica.read(buf, block.remove(less))
          less += (b3 - buf.readableBytes)
          replicas.add(rep)
        }
      }
      
      // Verify the correctness of the source digital signature
      val sigSlice = block.remove(less)
      if (!signature.verify(sigSlice))
        return new ReadResult("Incorrect signature!")
      
      val msg = new Message(version, record, body)
      msg.id = id
      msg.sigSlice = sigSlice
      msg.signature = signature
      msg.replicas = replicas
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
      Reply:    ''', type=«body.type», party=(«body.party.index», «body.party.quorum»)«IF body.propose !== null», index=«body.propose.index», f=«body.propose.fingerprint», round=«body.propose.round»«ENDIF»'''
      Get:      ''', index=«body.index», slices=«body.slices»'''
      Error:    ''', code=«body.code», error=«body.msg»'''
    }
  }
}
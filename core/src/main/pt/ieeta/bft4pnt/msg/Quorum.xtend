package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.security.PublicKey
import java.util.ArrayList
import java.util.List
import pt.ieeta.bft4pnt.crypto.HashHelper
import pt.ieeta.bft4pnt.crypto.KeyPairHelper

class Quorum implements ISection {
  public val String uid
  public val int t
  val List<PublicKey> parties
  
  def getN() { parties.size }
  
  new(String uid, int t, List<PublicKey> parties) {
    this.t = t
    this.parties = parties
    this.uid = uid
    verify
  }
  
  new(int t, List<PublicKey> parties) {
    this.t = t
    this.parties = parties
    this.uid = genUID
    verify
  }
  
  def getPartyKey(int party) {
    if (party >= parties.size)
      return null
    parties.get(party - 1)
  }
  
  private def verify() {
    if (n < 2*t + 1)
      throw new RuntimeException("Invalid quorum configuration")
  }
  
  private def String genUID() {
    val buf = PooledByteBufAllocator.DEFAULT.buffer(1024)
    write(buf)
    
    val data = newByteArrayOfSize(buf.readableBytes)
    buf.readBytes(data)
    HashHelper.digest(data)
  }
  
  override write(ByteBuf buf) {
    Message.writeString(buf, uid)
    buf.writeInt(t)
    
    buf.writeInt(parties.size)
    for (key : parties)
      Message.writeBytes(buf, key.encoded)
  }
  
  static def Quorum read(ByteBuf buf) {
    val uid = Message.readString(buf)
    val t = buf.readInt
    
    val number = buf.readInt
    val parties = new ArrayList<PublicKey>(number)
    for (n : 0 ..< number) {
      val bKey = Message.readBytes(buf)
      val key = KeyPairHelper.read(bKey)
      parties.add(key)
    }
    
    return new Quorum(uid, t, parties)
  }
}
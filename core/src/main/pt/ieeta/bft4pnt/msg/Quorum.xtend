package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.security.PublicKey
import java.util.ArrayList
import java.util.List
import pt.ieeta.bft4pnt.crypto.KeyPairHelper

class Quorum implements ISection {
  public val int t
  val List<PublicKey> parties
  
  def getN() { parties.size }
  
  new(int t, List<PublicKey> parties) {
    this.t = t
    this.parties = parties
    
    if (n < 2*t + 1)
      throw new RuntimeException('''Invalid quorum configuration! (n,t)=(«n»,«t»)''')
  }

  
  def getPartyKey(int party) {
    if (party > parties.size)
      return null
    parties.get(party - 1)
  }
  
  override write(ByteBuf buf) {
    buf.writeInt(t)
    
    buf.writeInt(parties.size)
    for (key : parties)
      Message.writeBytes(buf, key.encoded)
  }
  
  static def Quorum read(ByteBuf buf) {
    val t = buf.readInt
    
    val number = buf.readInt
    val parties = new ArrayList<PublicKey>(number)
    for (n : 0 ..< number) {
      val bKey = Message.readBytes(buf)
      val key = KeyPairHelper.read(bKey)
      parties.add(key)
    }
    
    return new Quorum(t, parties)
  }
}
package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.security.PublicKey
import java.util.ArrayList
import java.util.List
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import java.net.InetSocketAddress
import java.util.Collections

class Quorum implements ISection {
  public val int t
  public val List<PublicKey> parties
  public val List<InetSocketAddress> addresses
  
  def getN() { parties.size }
  
  new(int t, List<PublicKey> parties, List<InetSocketAddress> addresses) {
    this.t = t
    this.parties = Collections.unmodifiableList(parties)
    this.addresses = Collections.unmodifiableList(addresses)
    
    if (n < 2*t + 1 || parties.size !== addresses.size)
      throw new RuntimeException('''Invalid quorum configuration! (n,t)=(«n»,«t»)''')
  }
  
  def getPartyKey(int party) {
    if (party > parties.size)
      return null
    parties.get(party - 1)
  }
  
  def getPartyAddres(int party) {
    if (party > addresses.size)
      return null
    addresses.get(party - 1)
  }
  
  override write(ByteBuf buf) {
    buf.writeInt(t)
    buf.writeInt(parties.size)
    
    for (key : parties)
      Message.writeBytes(buf, key.encoded)
    
    for (address : addresses)
      Message.writeString(buf, '''«address.hostString»:«address.port»''')
  }
  
  static def Quorum read(ByteBuf buf) {
    val t = buf.readInt
    val number = buf.readInt
    
    val parties = new ArrayList<PublicKey>(number)
    val addresses = new ArrayList<InetSocketAddress>(number)
    
    for (n : 0 ..< number) {
      val bKey = Message.readBytes(buf)
      val key = KeyPairHelper.read(bKey)
      parties.add(key)
    }
    
    for (n : 0 ..< number) {  
      val bAddress = Message.readString(buf)
      val hostPort = bAddress.split(":")
      val address = new InetSocketAddress(hostPort.get(0), Integer.parseInt(hostPort.get(1)))
      addresses.add(address)
    }
    
    return new Quorum(t, parties, addresses)
  }
}
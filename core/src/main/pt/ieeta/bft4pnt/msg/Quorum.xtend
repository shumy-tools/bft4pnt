package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.net.InetSocketAddress
import java.security.PublicKey
import java.util.ArrayList
import java.util.Collections
import java.util.HashMap
import java.util.List
import pt.ieeta.bft4pnt.crypto.KeyPairHelper

class Quorum implements ISection {
  // helper storage, is not transmitted
  val parties =  new HashMap<String, Party>
  
  public val int index
  public val int t
  public val List<PublicKey> keys
  public val List<InetSocketAddress> addresses
  
  def getN() { keys.size }
  
  new(int index, int t, List<PublicKey> keys, List<InetSocketAddress> addresses) {
    this.index = index
    this.t = t
    this.keys = Collections.unmodifiableList(keys)
    this.addresses = Collections.unmodifiableList(addresses)
    
    if (n < 2*t + 1 || keys.size !== addresses.size)
      throw new RuntimeException('''Invalid quorum configuration! (n,t)=(«n»,«t»)''')
    
    var party = 1
    for (key : keys) {
      val sKey = KeyPairHelper.encode(key)
      parties.put(sKey, new Party(party, index))
      party++
    }
    
    if (keys.size !== parties.size)
      throw new RuntimeException('''Repeated keys in the quorum configuration!''')
  }
  
  def boolean contains(String party) {
    parties.containsKey(party)
  }
  
  def List<Party> getAllParties() {
    parties.values.toList
  }
  
  def Party getParty(String key) {
    parties.get(key)
  }
  
  def getPartyKey(int party) {
    if (party > keys.size)
      return null
    keys.get(party - 1)
  }
  
  def getPartyAddress(int party) {
    if (party > addresses.size)
      return null
    addresses.get(party - 1)
  }
  
  override write(ByteBuf buf) {
    buf.writeInt(index)
    buf.writeInt(t)
    buf.writeInt(keys.size)
    
    for (key : keys)
      Message.writeBytes(buf, key.encoded)
    
    for (address : addresses)
      Message.writeString(buf, '''«address.hostString»:«address.port»''')
  }
  
  static def Quorum read(ByteBuf buf) {
    val index = buf.readInt
    val t = buf.readInt
    val number = buf.readInt
    
    val keys = new ArrayList<PublicKey>(number)
    val addresses = new ArrayList<InetSocketAddress>(number)
    
    for (n : 0 ..< number) {
      val bKey = Message.readBytes(buf)
      val key = KeyPairHelper.read(bKey)
      keys.add(key)
    }
    
    for (n : 0 ..< number) {  
      val bAddress = Message.readString(buf)
      val hostPort = bAddress.split(":")
      val address = new InetSocketAddress(hostPort.get(0), Integer.parseInt(hostPort.get(1)))
      addresses.add(address)
    }
    
    return new Quorum(index, t, keys, addresses)
  }
}
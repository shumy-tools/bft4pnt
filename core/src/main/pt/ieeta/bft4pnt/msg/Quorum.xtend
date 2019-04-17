package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.net.InetSocketAddress
import java.security.PublicKey
import java.util.ArrayList
import java.util.Collections
import java.util.HashMap
import java.util.List
import java.util.TreeMap
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import java.util.Map

class Quorum implements ISection {
  public val int index
  public val int t
  public val Map<String, QuorumParty> parties
  
  def getN() { parties.size }
  
  new(int index, int t, List<QuorumParty> qParties) {
    this.index = index
    this.t = t
    
    val parties = new TreeMap<String, QuorumParty>
    for (qP : qParties)
      parties.put(KeyPairHelper.encode(qP.key), qP)
    
    this.parties = Collections.unmodifiableMap(parties)
    
    if (n < 2*t + 1)
      throw new RuntimeException('''Invalid quorum configuration! (n,t)=(«n»,«t»)''')
  }
  
  def boolean contains(String party) {
    parties.containsKey(party)
  }
  
  def List<String> getAllParties() {
    parties.keySet.toList
  }
  
  def getPartyKey(String party) {
    parties.get(party)?.key
  }
  
  def getPartyAddress(String party) {
    parties.get(party)?.address
  }
  
  def Quorum add(List<QuorumParty> qParties) {
    val all = new HashMap<String, QuorumParty> => [
      putAll(this.parties)
      for (qP : qParties)
        put(KeyPairHelper.encode(qP.key), qP)
    ]
    
    return new Quorum(index + 1, t, all.values.toList)
  }
  
  override write(ByteBuf buf) {
    buf.writeInt(index)
    buf.writeInt(t)
    buf.writeInt(parties.size)
    
    for (party : parties.values)
      party.write(buf)
  }
  
  static def Quorum read(ByteBuf buf) {
    val index = buf.readInt
    val t = buf.readInt
    val number = buf.readInt
    
    val qParties = new ArrayList<QuorumParty>(number)
    for (n : 0 ..< number) {
      val qP = QuorumParty.read(buf)
      qParties.add(qP)
    }
    
    return new Quorum(index, t, qParties)
  }
}

@FinalFieldsConstructor
class QuorumParty implements ISection {
  public val PublicKey key
  public val InetSocketAddress address
  
  def String strKey() {
    KeyPairHelper.encode(key)
  }
  
  override write(ByteBuf buf) {
    Message.writeBytes(buf, key.encoded)
    Message.writeString(buf, '''«address.hostString»:«address.port»''')
  }
  
  static def QuorumParty read(ByteBuf buf) {
    val bKey = Message.readBytes(buf)
    val key = KeyPairHelper.read(bKey)
    
    val bAddress = Message.readString(buf)
    val hostPort = bAddress.split(":")
    val address = new InetSocketAddress(hostPort.get(0), Integer.parseInt(hostPort.get(1)))
    
    return new QuorumParty(key, address)
  }
}

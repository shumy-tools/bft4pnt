package bft4pnt

import java.net.InetSocketAddress
import java.security.PublicKey
import java.security.Security
import java.util.ArrayList
import java.util.HashMap
import java.util.List
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.PNTServer
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.QuorumConfig
import pt.ieeta.bft4pnt.spi.MemoryStore

@FinalFieldsConstructor
class InitQuorum {
  val MessageBroker client
  val List<InetSocketAddress> parties
  
  static def InitQuorum init(QuorumConfig quorum) {
    Security.addProvider(new BouncyCastleProvider)
    
    val partyKeys = new HashMap<Integer, PublicKey>
    val (Integer)=>PublicKey resolver = [
      val key = partyKeys.get(it)
      if (key === null)
        throw new RuntimeException('''No key for party: «it»''')
      key
    ]
    
    val parties = new ArrayList<InetSocketAddress>
    for (i : 0 ..< quorum.n) {
      val inet = new InetSocketAddress("127.0.0.1", 3001 + i)
      val keys = KeyPairHelper.genKeyPair
      partyKeys.put(i + 1, keys.public)
      
      val broker = new MessageBroker(inet, keys)
      val store = new MemoryStore(quorum)
      val (Message)=>boolean authorizer = [ true ]
      
      val pnt = new PNTServer(i + 1, broker, store, authorizer, resolver)
      parties.add(inet)
      
      pnt.start
      while (!pnt.ready)
        Thread.sleep(100)
    }
    
    val inet = new InetSocketAddress("127.0.0.1", 3000)
    val keys = KeyPairHelper.genKeyPair
    val client = new MessageBroker(inet, keys)
    
    return new InitQuorum(client, parties)
  }
  
  def void send(int party, Message msg) {
    client.send(parties.get(party - 1), msg)
  }
  
  def void start((Integer, Message)=>void handler) {
    client.start([], [ inetSource, reply |
      handler.apply(inetSource.port - 3000, reply)
    ])
  }
}
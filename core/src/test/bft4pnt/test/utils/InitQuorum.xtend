package bft4pnt.test.utils

import java.net.InetSocketAddress
import java.security.KeyPair
import java.security.Security
import java.util.ArrayList
import java.util.List
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.PNTServer
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Quorum

@FinalFieldsConstructor
class InitQuorum {
  val int port
  val MessageBroker client
  
  public val Quorum quorum
  val List<InetSocketAddress> parties
  
  static def InitQuorum init(int port, int n, int t) {
    Security.addProvider(new BouncyCastleProvider)
    
    val parties = new ArrayList<KeyPair>
    val inets = new ArrayList<InetSocketAddress>
    for (i : 0 ..< n) {
      val inet = new InetSocketAddress("127.0.0.1", port + 1 + i)
      val keys = KeyPairHelper.genKeyPair
      parties.add(keys)
      inets.add(inet)
    }
    
    val quorum = new Quorum(t, parties.map[public])
    for (i : 0 ..< n) {
      val broker = new MessageBroker(inets.get(i), parties.get(i))
      val storeMng = new MemoryStoreManager(quorum)
      val (Message)=>boolean authorizer = [ true ]
      
      val pnt = new PNTServer(i + 1, broker, storeMng, authorizer)
      pnt.start
      
      while (!pnt.ready)
        Thread.sleep(100)
    }
    
    val inet = new InetSocketAddress("127.0.0.1", port)
    val keys = KeyPairHelper.genKeyPair
    val client = new MessageBroker(inet, keys)
    
    return new InitQuorum(port, client, quorum, inets)
  }
  
  def void send(int party, Message msg) {
    client.send(parties.get(party - 1), msg)
  }
  
  def void start((Integer, Message)=>void handler) {
    client.start([], [ inetSource, reply |
      handler.apply(inetSource.port - port, reply)
    ])
  }
}
package bft4pnt.test.utils

import java.net.InetSocketAddress
import java.security.KeyPair
import java.security.Security
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicInteger
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.PNTServer
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Quorum
import pt.ieeta.bft4pnt.spi.PntDatabase
import pt.ieeta.bft4pnt.spi.IStoreManager
import java.util.List

@FinalFieldsConstructor
class InitQuorum {
  val int port
  val MessageBroker client
  val List<PNTServer> servers
  val Quorum quorum
  
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
    
    val servers = new ArrayList<PNTServer>
    for (i : 0 ..< n) {
      val dbName = '''DB-«port»-i'''
      PntDatabase.set(dbName, new InMemoryStoreMng, new InMemoryFileMng)
      
      val broker = new MessageBroker(inets.get(i), parties.get(i))
      val (Message)=>boolean authorizer = [ true ]
      
      val pnt = new PNTServer(parties.get(i), dbName, broker, authorizer)
      servers.add(pnt)
      while (!pnt.ready)
        Thread.sleep(100)
    }
    
    val inet = new InetSocketAddress("127.0.0.1", port)
    val keys = KeyPairHelper.genKeyPair
    val quorum = new Quorum(0, t, parties.map[public], inets)
    
    val client = new MessageBroker(inet, keys)
    client.logInfoFilter = [false]
    
    return new InitQuorum(port, client, servers, quorum)
  }
  
  def replicator(int party) {
    val srv = servers.get(party - 1)
    return srv.replicator
  }
  
  def void send(int party, Message msg) {
    client.send(quorum.getPartyAddress(party), msg)
  }
  
  def void start((Integer, Message)=>void handler, ()=>void startTest) {
    val counter = new AtomicInteger(0)
    client.start[ inetSource, reply |
      if (counter.incrementAndGet === quorum.n) {
        println("Quorum set, starting test.")
        startTest.apply
      }
      
      if (reply.id > 0L)
        handler.apply(inetSource.port - port, reply)
    ]
    
    // set quorum config
    val insert = Insert.create(0L, IStoreManager.localStore, IStoreManager.quorumAlias, new Data(quorum))
    for (party : 1 .. quorum.n)
      send(party, insert)
  }
}
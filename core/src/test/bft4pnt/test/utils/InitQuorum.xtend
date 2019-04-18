package bft4pnt.test.utils

import java.net.InetSocketAddress
import java.security.Security
import java.util.ArrayList
import java.util.List
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
import pt.ieeta.bft4pnt.msg.QuorumParty
import pt.ieeta.bft4pnt.spi.PntDatabase
import pt.ieeta.bft4pnt.spi.Store
import pt.ieeta.bft4pnt.spi.StoreManager
import org.eclipse.xtend.lib.annotations.Accessors
import org.slf4j.LoggerFactory
import io.netty.buffer.ByteBuf

@FinalFieldsConstructor
class InitQuorumParty {
  public val QuorumParty party
  public val PNTServer server
}

@FinalFieldsConstructor
class InitQuorum {
  static val logger = LoggerFactory.getLogger(InitQuorum.simpleName)
  
  static val (Message)=>boolean authorizer = [ true ]
  
  val int port
  val MessageBroker client
  val List<InitQuorumParty> parties
  
  @Accessors(PUBLIC_GETTER) var String quorumRec
  @Accessors(PUBLIC_GETTER) var Quorum quorum
  @Accessors(PUBLIC_GETTER) var String finger
  
  new(int port, MessageBroker client, List<InitQuorumParty> parties, Quorum quorum) {
    this(port, client, parties)
    
    this.quorum = quorum
    this.finger = new Data(quorum).fingerprint
    this.quorumRec = finger
  }
  
  static def InitQuorumParty newParty(int port, int party) {
    val dbName = '''DB-«port»-«party»'''
    PntDatabase.set(dbName, new InMemoryStoreMng, new InMemoryFileMng)
    
    val keys = KeyPairHelper.genKeyPair
    val inet = new InetSocketAddress("127.0.0.1", port + party)
      
    val broker = new MessageBroker(inet, keys)
    val pnt = new PNTServer(keys, dbName, broker, authorizer)
    while (!pnt.ready)
      Thread.sleep(100)
      
    return new InitQuorumParty(new QuorumParty(keys.public, inet ), pnt)
  }
  
  static def InitQuorum init(int port, int n, int t) {
    Security.addProvider(new BouncyCastleProvider)
    
    val parties = new ArrayList<InitQuorumParty>
    for (i : 0 ..< n) {
      val iqp = newParty(port, i + 1)
      parties.add(iqp)
    }
    
    val quorum = new Quorum(0, t, parties.map[party].clone)
    
    val inet = new InetSocketAddress("127.0.0.1", port)
    val keys = KeyPairHelper.genKeyPair
    val client = new MessageBroker(inet, keys)
    client.logInfoFilter = [false]
    
    return new InitQuorum(port, client, parties, quorum)
  }
  
  def getPartyAtIndex(int party) {
    parties.get(party - 1).party.strKey
  }
  
  def addParty(InitQuorumParty party) {
    parties.add(party)
    this.quorum = quorum.add(#[party.party])
    this.finger = new Data(quorum).fingerprint
  }
  
  def replicator(int party) {
    val srv = parties.get(party - 1).server
    return srv.replicator
  }
  
  def void send(int party, Message msg) {
    val qParty = parties.get(party - 1)
    client.send(qParty.party.address, msg)
  }
  
  def ByteBuf write(Message msg) {
    client.write(msg)
  }
  
  def void directSend(int party, ByteBuf data) {
    val qParty = parties.get(party - 1)
    client.directSend(qParty.party.address, data)
  }
  
  def void start((Integer, Message)=>void handler, ()=>void startTest) {
    val counter = new AtomicInteger(0)
    client.start[ inetSource, reply |
      counter.incrementAndGet
      
      if (counter.get === quorum.n) {
        // set client quorum index
        val insert = Insert.create(0L, "udi-1", Store.QUORUM_ALIAS, new Data(0))
        for (party : 1 .. quorum.n)
          send(party, insert)
      }
      
      if (counter.get === 2*quorum.n) {
        logger.info("Quorum set, starting test.")
        startTest.apply
      }
      
      if (counter.get > 2*quorum.n)
        handler.apply(inetSource.port - port, reply)
    ]
    
    // set quorum config
    val insert = Insert.create(0L, StoreManager.LOCAL_STORE, Store.QUORUM_ALIAS, new Data(quorum))
    for (party : 1 .. quorum.n)
      send(party, insert)
  }
  
  def void stop() {
    parties.forEach[server.stop]
  }
}

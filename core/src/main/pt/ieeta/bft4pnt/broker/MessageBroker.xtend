package pt.ieeta.bft4pnt.broker

import java.net.InetSocketAddress
import java.security.PrivateKey
import java.security.PublicKey
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.msg.Message

class MessageBroker {
  static val logger = LoggerFactory.getLogger(MessageBroker.simpleName)
  
  val DataBroker db
  
  val String udi
  val PrivateKey prvKey
  
  val (InetSocketAddress)=>PublicKey addressResolver
  
  new(InetSocketAddress address, PrivateKey prvKey, (InetSocketAddress)=>PublicKey addressResolver) {
    this.db = new DataBroker(address)
    this.prvKey = prvKey
    
    val pubKey = addressResolver.apply(address)
    this.udi = KeyPairHelper.encode(pubKey)
    
    this.addressResolver = addressResolver
  }
  
  def void start((Message)=>void handler) {
    db.start([
      logger.info("MSG-BROKER-READY {} on {}", udi, '''«hostString»:«port»''')
    ], [inetSource, data |
      val source = addressResolver.apply(inetSource)
      val result = Message.read(data, source)
      if (result.hasError) {
        logger.info("MSG-ERROR: {}", result.error.msg)
        
        val error = result.error.toMessage.write(prvKey)
        db.send(inetSource, error)
        
        return;
      }
      
      result.msg.address = inetSource
      logger.info("MSG-RECEIVED: {} from {}", result.msg, result.msg.source)
      handler.apply(result.msg)
    ])
  }
  
  def void send(Message msg) {
    val data = msg.write(prvKey)
    
    while (!db.ready)
      Thread.sleep(100)
    
    db.send(msg.address, data)
    logger.info("MSG-SENT: {} to {}", msg, '''«msg.address.hostString»:«msg.address.port»''')
  }
}
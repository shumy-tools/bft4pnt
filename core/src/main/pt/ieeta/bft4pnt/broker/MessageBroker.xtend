package pt.ieeta.bft4pnt.broker

import java.net.InetSocketAddress
import pt.ieeta.bft4pnt.msg.Message
import java.security.PublicKey
import java.security.PrivateKey
import org.slf4j.LoggerFactory

class MessageBroker {
  static val logger = LoggerFactory.getLogger(MessageBroker)
  
  val DataBroker db
  val PrivateKey prvKey
  val PublicKey pubKey
  
  new(InetSocketAddress address, PrivateKey prvKey, PublicKey pubKey) {
    this.db = new DataBroker(address)
    this.prvKey = prvKey
    this.pubKey = pubKey
  }
  
  def void start((Message)=>void handler) {
    db.start[source, data |
      logger.info("MSG-RECEIVED: {}", source)
      
      val result = Message.read(data, pubKey)
      if (result.hasError) {
        logger.info("MSG-ERROR: {}", result.error.msg)
        
        val error = result.error.toMessage.write(prvKey)
        db.send(source, error)
        
        return;
      }
      
      result.msg.address = source
      handler.apply(result.msg)
    ]
  }
  
  def void send(Message msg) {
    val data = msg.write(prvKey)
    
    db.send(msg.address, data)
    logger.info("MSG-SENT: {} -> {}", msg.id, msg.address)
  }
}
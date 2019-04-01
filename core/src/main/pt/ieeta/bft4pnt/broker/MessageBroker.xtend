package pt.ieeta.bft4pnt.broker

import java.net.InetSocketAddress
import java.security.KeyPair
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.msg.Error
import pt.ieeta.bft4pnt.msg.Message

class MessageBroker {
  static val logger = LoggerFactory.getLogger(MessageBroker.simpleName)
  
  val DataBroker db
  
  val String udi
  val KeyPair keys
  
  new(InetSocketAddress address, KeyPair keys) {
    this.db = new DataBroker(address)
    this.keys = keys
    this.udi = KeyPairHelper.encode(keys.public)
  }
  
  def void start((Message)=>void handler) {
    db.start([
      logger.info("MSG-BROKER-READY {} on {}", udi, '''«hostString»:«port»''')
    ], [inetSource, data |
      try {
        val result = Message.read(data)
        if (result.hasError) {
          logger.error("MSG-ERROR: {}", result.error.msg)
          val error = result.error.toMessage.write(keys)
          db.send(inetSource, error)
          return;
        }
        
        result.msg.address = inetSource
        logger.info("MSG-RECEIVED: {} from {}", result.msg, '''«result.msg.address.hostString»:«result.msg.address.port»''')
        handler.apply(result.msg)
      } catch(Throwable ex) {
        logger.error("MSG-ERROR: {}", ex.message)
        ex.printStackTrace
        
        val error = Error.internal(ex.message)
        db.send(inetSource, error.toMessage.write(keys))
      }
    ])
  }
  
  def void send(Message msg) {
    val data = msg.write(keys)
    
    while (!db.ready)
      Thread.sleep(100)
    
    db.send(msg.address, data)
    logger.info("MSG-SENT: {} to {}", msg, '''«msg.address.hostString»:«msg.address.port»''')
  }
}
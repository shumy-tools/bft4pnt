package pt.ieeta.bft4pnt.broker

import java.net.InetSocketAddress
import java.security.KeyPair
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.msg.Error
import pt.ieeta.bft4pnt.msg.Message

class MessageBroker {
  static val logger = LoggerFactory.getLogger(MessageBroker.simpleName)
  
  val DataBroker db
  val KeyPair keys
  
  new(InetSocketAddress address, KeyPair keys) {
    this.db = new DataBroker(address)
    this.keys = keys
  }
  
  def isReady() { db.ready }
  
  def void start((InetSocketAddress, Message)=>void handler) {
    db.start[inetSource, data |
      try {
        val result = Message.read(data)
        if (result.hasError) {
          logger.error("MSG-ERROR: {}", result.error.msg)
          val error = result.error.toMessage.write(keys)
          db.send(inetSource, error)
          return;
        }
        
        logger.info("MSG-RECEIVED: {} from {}", result.msg, '''«inetSource.hostString»:«inetSource.port»''')
        handler.apply(inetSource, result.msg)
      } catch(Throwable ex) {
        logger.error("MSG-ERROR: {}", ex.message)
        ex.printStackTrace
        
        val error = Error.internal(ex.message)
        db.send(inetSource, error.toMessage.write(keys))
      }
    ]
  }
  
  def void send(InetSocketAddress inetTarget, Message msg) {
    //TODO: encrypt message?
    val data = msg.write(keys)
    
    while (!db.ready)
      Thread.sleep(100)
    
    db.send(inetTarget, data)
    logger.info("MSG-SENT: {} to {}", msg, '''«inetTarget.hostString»:«inetTarget.port»''')
  }
}
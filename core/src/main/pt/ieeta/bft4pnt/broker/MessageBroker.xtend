package pt.ieeta.bft4pnt.broker

import java.net.InetSocketAddress
import java.security.KeyPair
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.msg.Error
import pt.ieeta.bft4pnt.msg.Message
import java.util.concurrent.atomic.AtomicReference

class MessageBroker {
  static val logger = LoggerFactory.getLogger(MessageBroker.simpleName)
  
  val lFilter = new AtomicReference<(Message)=>boolean>
  val DataBroker db
  val KeyPair keys
  
  new(InetSocketAddress address, KeyPair keys) {
    this.db = new DataBroker(address)
    this.keys = keys
  }
  
  def isReady() { db.ready }
  
  def void setLogInfoFilter((Message)=>boolean filter) {
    lFilter.set(filter)
  }
  
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
        
        log(result.msg)[logger.info("MSG-RECV: {} from {}", result.msg, '''«inetSource.hostString»:«inetSource.port»''')]
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
    log(msg)[logger.info("MSG-SENT: {} to {}", msg, '''«inetTarget.hostString»:«inetTarget.port»''')]
  }
  
  private def log(Message msg, ()=>void log) {
    val filter = lFilter.get
    if (filter === null || filter.apply(msg) === true)
      log.apply
  }
}
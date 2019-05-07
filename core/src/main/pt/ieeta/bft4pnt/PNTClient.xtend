package pt.ieeta.bft4pnt

import java.util.List
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.broker.ClientDataChannel
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.msg.Get
import pt.ieeta.bft4pnt.msg.HasSlices
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Quorum
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Slices
import pt.ieeta.bft4pnt.msg.Update
import java.util.Random

class PNTClient {
  static val logger = LoggerFactory.getLogger(PNTClient.simpleName)
  
  val msgId = new AtomicLong(0L)
   
  val String udi
  val Quorum quorum
  
  val MessageBroker broker
  val ClientDataChannel channel
  
  val retrieves = new ConcurrentHashMap<String, Retrieve>
  
  new(String udi, Quorum quorum, MessageBroker broker, ClientDataChannel channel) {
    this.udi = udi
    this.quorum = quorum
    
    this.broker = broker
    this.channel = channel
    
    start
  }
  
  def isReady() { broker.ready }
  
  def void start() {
    broker.start
    broker.addListener[ inetSource, msg |
      logger.info("RCV: {} form {}", msg, inetSource.port)
      
      val ret = retrieves.get(msg.record.fingerprint)
      if (ret === null) {
        logger.error("No corresponding retrieve request available!")
        return;
      }
      
      // ignore these
      if (ret.ready.get) return;
      
      switch msg.type {
        case INSERT,
        case UPDATE: ret.ok(msg)
        case REPLY: ret.reply(msg, msg.body as Reply)
        
        case ERROR: logger.error("Client received an error message: {}", msg)
        default: logger.error("Client invalid reply type: {}", msg.type)
      }
    ]
    
    channel.start[ record, index, slice |
      val ret = retrieves.get(record)
      if (ret === null) {
        logger.warn("No retrieve request found for record: (record={})", record)
        return;
      }
      
      if (slice !== null) {
        if (!ret.slices.get.slices.contains(slice)) {
          //TODO: identity party!
          logger.error("Invalid returned slice: (record={}, slice={})", record, slice)
          return;
        }
        
        ret.slicesReady.add(slice)
        if (ret.slicesReady.size == ret.slices.get.slices.size) {
          //TODO: if all slices are ready, then join slices in one data block
          
          logger.info("RETRIEVE-COMPLETED: (record={}, index={})", record, index)
          retrieves.remove(record)
          ret.promise.complete(true)
        }
      } else {
          logger.info("RETRIEVE-COMPLETED: (record={}, index={})", record, index)
          retrieves.remove(record)
          ret.promise.complete(true)
      }
    ]
  }
  
  def get(String record) {
    val ret = new Retrieve
    retrieves.put(record, ret)
    
    Executors.newCachedThreadPool.submit[
      val msg = Get.create(msgId.incrementAndGet, udi, record)
      for (adr : quorum.allAddresses)
        broker.send(adr, msg)
    ]
    
    return ret.promise
  }
  
  private def void ok(Retrieve ret, Message msg) {
    for (strParty : msg.replicas.map[strSource]) {
      // ignore non quorum replicas
      if (quorum.contains(strParty))
        ret.replies.put(strParty, msg)
    }
    
    if (ret.replies.size >= (quorum.n - quorum.t)) {
      ret.ready.set = true
      ret.ifReady(msg.record.fingerprint)
    }
  }
  
  private def void reply(Retrieve ret, Message msg, Reply reply) {
    if (!quorum.contains(reply.strParty)) {
      logger.error("Not part of the quorum: {}", reply.strParty)
      return;
    }
    
    ret.replies.put(reply.strParty, msg)
    if (ret.replies.size >= (quorum.n - quorum.t)) {
      ret.ready.set = true
      ret.ifReady(msg.record.fingerprint)
    }
    
    // TODO: force replication sync?
  }
  
  private def ifReady(Retrieve ret, String record) {
    var topIdx = -1
    for (reply: ret.replies.values) {
      val idx = reply.msgIndex
      if (idx > topIdx)
        topIdx = idx
    }
    
    // find valid replicas and retrieve slices
    if (topIdx != -1) {
      val max = topIdx
      val replicas = ret.replies.filter[key, reply | reply.msgIndex == max ]
      if (replicas.size > 0)
        ret.retrieve(replicas.keySet.toList, (replicas.values.head.body as HasSlices).slices, record, max)
    }
  }
  
  private def retrieve(Retrieve ret, List<String> replicas, Slices slices, String record, int index) {
    ret.slices.set = slices
    
    Executors.newCachedThreadPool.submit[
      val mid = msgId.incrementAndGet
      
      // get whole data block from a random party
      if (slices.slices.size == 0) {
        val i = (new Random).nextInt(replicas.size)
        val party = replicas.get(i)
        val adr = quorum.getPartyAddress(party)
        
        //request slice via stream channel
        logger.info("RETRIEVE-ALL: (record={}, index={}, party={})", record, index, party)
        val msg = Get.create(mid, udi, record, index, -1)
        channel.request(adr, msg)
        return;
      }
      
      // distribute slice requests
      for (i : 0 ..< slices.slices.size) {
        val party = replicas.get(i % replicas.size)
        val adr = quorum.getPartyAddress(party)
        
        //request slice via stream channel
        logger.info("RETRIEVE-SLICE: (record={}, index={}, party={}, slice={})", record, index, party, i)
        val msg = Get.create(mid, udi, record, index, i)
        channel.request(adr, msg)
      }
    ]
  }
  
  private def getMsgIndex(Message msg) {
    if (msg.type == Message.Type.INSERT)
      0
    else if (msg.type == Message.Type.UPDATE)
      (msg.body as Update).propose.index
    else
      -1
  }
}

class Retrieve {
  public val promise = new CompletableFuture<Boolean>
  public val ready = new AtomicBoolean(false)
  public val replies = new ConcurrentHashMap<String, Message>
  
  public val slices = new AtomicReference<Slices>
  public val slicesReady = ConcurrentHashMap.<String>newKeySet
}

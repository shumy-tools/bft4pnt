package bft4pnt.test

import bft4pnt.test.utils.InitQuorum
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.PrintStream
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import net.jodah.concurrentunit.Waiter
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update

import static java.lang.System.*

class EvaluationTest {
  val udi = "udi-1"
  
  def outputFile(String name) {
    new PrintStream(new BufferedOutputStream(new FileOutputStream(name)), true)
  }
  
  @Test
  def void testTransactions() {
    try {
      System.setOut = outputFile("eval.txt")
      System.setErr = outputFile("error.txt")
      
      val eval = System.getenv("EVAL")
      if (eval === null || !Boolean.parseBoolean(eval))
        return;
      
      val $1 = System.getenv("PARTIES")
      val parties = Integer.parseInt($1)
      
      val $2 = System.getenv("BATCH")
      val batch = Integer.parseInt($2)
      
      val root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
      root.level = Level.ERROR
      
      println('''Eval transactions -> (parties=«parties», batch=«batch»)''')
      evalInserts(5000, parties, batch)
      evalUpdates(6000, parties, batch)
    } catch (Throwable ex) {
      ex.printStackTrace
    }
  }
  
  def void evalInserts(int port, int n, int batch) {
    val waiter = new Waiter
    val net = InitQuorum.init(port, n, 1)
    
    val sent = new AtomicInteger(0)
    val replies = new AtomicInteger(0)
    val msgID = new AtomicLong(0L)
    
    val time = new AtomicLong(0L)
    net.start([ party, reply |
      waiter.assertTrue(reply.body instanceof Reply)
      replies.incrementAndGet
      if (replies.get > n*batch) {
        waiter.resume
        return;
      }
      
      //println('''REPLY: «reply» -> «sent.get» / «replies.get»''')
      if (replies.get == sent.get) {
        val insert = Insert.create(msgID.incrementAndGet, udi, "eval", new Data(UUID.randomUUID.toString))
        val buf = net.write(insert)
        for (sendTo : 1 .. n) {
          net.directSend(sendTo, buf)
          sent.incrementAndGet
        }
        buf.release
      }
    ], [
      time.set = System.currentTimeMillis
      val insert = Insert.create(msgID.incrementAndGet, udi, "eval", new Data(UUID.randomUUID.toString))
      for (sendTo : 1 .. n) {
        net.send(sendTo, insert)
        sent.incrementAndGet
      }
    ])
    
    waiter.await(1_000_000)
    val delta = (System.currentTimeMillis - time.get) / 1000.0
    println('''  INSERTS IN «delta»s''')
    net.stop
  }
  
  def void evalUpdates(int port,int n, int batch) {
    val waiter = new Waiter
    val net = InitQuorum.init(port, n, 1)
    
    val insert = Insert.create(0L, udi, "eval", new Data(UUID.randomUUID.toString))
    val propose = new AtomicReference<Message>
    val data = new AtomicReference<Data>
    
    val updates = new AtomicInteger(0)
    val counter = new AtomicInteger(0)
    val msgID = new AtomicLong(0L)
    val round = new AtomicLong(0L)
    
    val voteReplies = new ConcurrentHashMap<String, Message>
    val time = new AtomicLong(0L)
    net.start([ party, reply |
      waiter.assertTrue(reply.body instanceof Reply)
      if (updates.get > batch) {
        waiter.resume
        return;
      }
      
      counter.incrementAndGet
      val rBody = reply.body as Reply
      //if (counter.get % 1000 == 0)
      //  println('''REPLY: «reply» -> «counter.get»''')
      
      // propose change
      if (rBody.type == Reply.Type.ACK && (counter.get + n) % (2*n) == 0) {
        round.incrementAndGet
        data.set = new Data(UUID.randomUUID.toString)
        propose.set = Propose.create(msgID.incrementAndGet, udi, insert.record.fingerprint, data.get.fingerprint, round.get as int, round.get)
        
        //println('''PROPOSE: «propose.get» ''')
        val buf = net.write(propose.get)
        for (sendTo : 1 .. n)
          net.directSend(sendTo, buf)
        buf.release
      }
      
      // commit change
      if (rBody.type == Reply.Type.VOTE) {
        voteReplies.put(rBody.strParty, reply)
        if (voteReplies.size == n) {
          val update = Update.create(msgID.incrementAndGet, udi, insert.record.fingerprint, 0, propose.get.body as Propose, voteReplies, data.get)
          //println('''UPDATE: «update» -> «updates.get»''')
          updates.incrementAndGet
          
          val buf = net.write(update)
          for (sendTo : 1 .. n)
            net.directSend(sendTo, buf)
          voteReplies.clear
          buf.release
        }
      }
    ], [
      time.set = System.currentTimeMillis
      for (sendTo : 1 .. n) {
        insert.id = msgID.incrementAndGet
        net.send(sendTo, insert)
      }
    ])
    
    waiter.await(10_000_000)
    val delta = (System.currentTimeMillis - time.get) / 1000.0
    println('''  UPDATEs IN «delta»s''')
    net.stop
  }
}
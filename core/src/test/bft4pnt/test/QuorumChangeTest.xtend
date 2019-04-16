package bft4pnt.test

import bft4pnt.test.utils.InitQuorum
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import net.jodah.concurrentunit.Waiter
import org.junit.jupiter.api.Test
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update

import static extension bft4pnt.test.utils.WaiterAssertExtensions.*
import pt.ieeta.bft4pnt.spi.StoreManager
import java.util.concurrent.atomic.AtomicReference

class QuorumChangeTest {
  
  @Test
  def void testEvolution() {
    /*  1     2     3     4     5
        1Pa   1Pa   1Pa         X
        1Ua   1Ua               X
    -----------------------------
        2Pq   2Pq         2Pq   X
        2Uq   2Uq         2Uq   OK
    -----------------------------
        x3Pb
    R
    */
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3020, 4, 1)
    
    val udi = "udi-1"
    val d1 = new Data("d1")
    val d2 = new Data("d2")
    
    val insert = Insert.create(1L, udi, "test", new Data("data-i"))
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, d1.fingerprint, 1, 1L)
    val pb3 = Propose.create(6L, udi, insert.record.fingerprint, d2.fingerprint, 2, 3L)
    val qp = new AtomicReference<Message>
    
    val voteReplies = new ConcurrentHashMap<String, Message>
    net.start([ party, reply |
      if (reply.id == 1L && #[1,2,3].contains(party))
        net.send(party, pa1)
      
      if (reply.id == 2L && #[1,2,3].contains(party)) {
        waiter.assertVote(reply, 1L)
        val rVote = reply.body as Reply
        voteReplies.put(rVote.strParty, reply)
        if (voteReplies.size == 3) {
          val ua1 = Update.create(3L, udi, insert.record.fingerprint, 0, pa1.body as Propose, voteReplies, d1)
          for (sendTo : 1 .. 3)
            net.send(sendTo, ua1)
          voteReplies.clear
        }
      }
      
      if (reply.id == 3L) {
        waiter.assertAck(reply)
        if (counter.incrementAndGet == 3) {
          println("----------------CHANGE QUORUM----------------")
          val qParty = InitQuorum.newParty(3020, 5)
          net.addParty(qParty)
          
          qp.set = Propose.create(4L, StoreManager.LOCAL_STORE, net.quorumRec, net.finger, 1, 2L)
          for (sendTo : 1 .. 3)
            net.send(sendTo, qp.get)
        }
      }
      
      if (reply.id == 4L) {
        waiter.assertVote(reply, 2L)
        val rVote = reply.body as Reply
        voteReplies.put(rVote.strParty, reply)
        if (voteReplies.size == 3) {
          val ua1 = Update.create(5L, StoreManager.LOCAL_STORE, net.quorumRec, 0, qp.get.body as Propose, voteReplies, new Data(net.quorum))
          for (sendTo : 1 .. 3)
            net.send(sendTo, ua1)
          voteReplies.clear
        }
      }
      
      if (reply.id == 5L) {
        waiter.assertAck(reply)
        if (counter.incrementAndGet == 6)
          net.send(1, pb3)
      }
      
      if (reply.id == 6L) {
        waiter.assertError(reply, "Store in incorrect quorum!")
        counter.incrementAndGet
        
        println('''----------------START-REP 1----------------''')
        net.replicator(1).replicate
      }
      
    ], [
      for (party : 1 .. 4)
        net.send(party, insert)
    ])
    
    waiter.await(400000)
    waiter.assertTrue(ok.get)
  }
  
  @Test
  def void testEvolutionFail() {
    /*  1     2     3     4     5
        1Pa   1Pa   1Pa         X
        1Ua   1Ua   2Pb   2Pb   X
    -----------------------------
        
    */
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3030, 4, 1)
    
    val udi = "udi-1"
    val d1 = new Data("d1")
    val d2 = new Data("d2")
    
    val insert = Insert.create(1L, udi, "test", new Data("data-i"))
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, d1.fingerprint, 1, 1L)
    val pb1 = Propose.create(4L, udi, insert.record.fingerprint, d2.fingerprint, 1, 2L)
    
    val voteReplies = new ConcurrentHashMap<String, Message>
    net.start([ party, reply |
      if (reply.id == 1L && #[1,2,3].contains(party))
        net.send(party, pa1)
      
      if (reply.id == 2L && #[1,2,3].contains(party)) {
        waiter.assertVote(reply, 1L)
        val rVote = reply.body as Reply
        
        if (rVote.propose.round == 1) {
          voteReplies.put(rVote.strParty, reply)
          if (voteReplies.size == 3) {
            val ua1 = Update.create(3L, udi, insert.record.fingerprint, 0, pa1.body as Propose, voteReplies, d1)
            for (sendTo : 1 .. 2)
              net.send(sendTo, ua1)
            voteReplies.clear
            
            for (sendTo : 3 .. 4)
              net.send(party, pb1)
          }
        }
      }
      
      if (reply.id == 3L) {
        waiter.assertAck(reply)
        counter.incrementAndGet
      }
      
      if (reply.id == 4L) {
        waiter.assertVote(reply, 2L)
        counter.incrementAndGet
      }
      
      if (counter.get == 4) {
        println("----------------CHANGE QUORUM----------------")
        val qParty = InitQuorum.newParty(3030, 5)
        net.addParty(qParty)
      }
      
    ], [
      for (party : 1 .. 4)
        net.send(party, insert)
    ])
    
    waiter.await(400000)
    waiter.assertTrue(ok.get)
  }
}
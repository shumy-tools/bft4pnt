package bft4pnt.test

import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger
import net.jodah.concurrentunit.Waiter
import org.junit.jupiter.api.Test
import pt.ieeta.bft4pnt.msg.Error
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update
import bft4pnt.test.utils.InitQuorum

class ConsensusTest {
  def void assertError(Waiter waiter, Message reply, String msg) {
    waiter.assertTrue(reply.body instanceof Error)
    waiter.assertEquals((reply.body as Error).msg, msg)
  }
  
  def void assertAck(Waiter waiter, Message reply) {
    waiter.assertTrue(reply.body instanceof Reply)
    waiter.assertEquals((reply.body as Reply).type, Reply.Type.ACK)
  }
  
  def void assertVote(Waiter waiter, Message reply, long round) {
    waiter.assertTrue(reply.body instanceof Reply)
    waiter.assertEquals((reply.body as Reply).type, Reply.Type.VOTE)
    waiter.assertEquals((reply.body as Reply).propose.round, round)
  }
  
  def void assertUpdate(Waiter waiter, Message reply, String rec, Propose propose) {
    waiter.assertTrue(reply.body instanceof Update)
    waiter.assertEquals(rec, reply.record.fingerprint)
    val update = reply.body as Update
    
    waiter.assertEquals(propose.index, update.propose.index)
    waiter.assertEquals(propose.fingerprint, update.propose.fingerprint)
    waiter.assertEquals(propose.round, update.propose.round)
  }
  
  def void assertNoData(Waiter waiter, Message reply) {
    waiter.assertTrue(reply.body instanceof Reply)
    waiter.assertEquals((reply.body as Reply).type, Reply.Type.NO_DATA)
  }
  
  @Test
  def void testBasicInsertUpdate() {
    /*  1     2     3     4
        2Pa   2Pa   2Pa   2Pa
        x1Ua  1Ua   x1Pa
    */
    
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3000, 4, 1)
    
    val udi = "udi-1"
    val insert = Insert.create(1L, udi, "test", "data-i")
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, "a", 1, 2L)
    
    val voteReplies = new HashMap<Integer, Message>
    net.start[ party, reply |
      if (reply.id == 1L)
        net.send(party, pa1)
      
      if (reply.id == 2L && #[1,2,3,4].contains(party)) {
        waiter.assertVote(reply, 2L)
        
        val rVote = reply.body as Reply
        voteReplies.put(rVote.party, reply)
        
        if (voteReplies.size == 2) {
          val u1 = Update.create(3L, udi, insert.record.fingerprint, net.quorum.uid, pa1.body as Propose, voteReplies)
          net.send(party, u1)
        }
        
        if (voteReplies.size == 3) {
          // should ignore lower rounds
          val p2 = Propose.create(4L, udi, insert.record.fingerprint, "data-3", 1, 1L)
          net.send(party, p2)
          
          val u2 = Update.create(5L, udi, insert.record.fingerprint, net.quorum.uid, pa1.body as Propose, voteReplies)
          net.send(party, u2)
        }
      }
      
      if (reply.id == 3L) {
        waiter.assertError(reply, "Not enough votes!")
        counter.incrementAndGet
      }
      
      if (reply.id == 4L) {
        waiter.assertVote(reply, 2L)
        counter.incrementAndGet
      }
      
      if (reply.id == 5L) {
        waiter.assertAck(reply)
        counter.incrementAndGet
      }
      
      if (counter.get == 3)
        waiter.resume
    ]
    
    for (party : 1 .. 4)
      net.send(party, insert)
    waiter.await(2000)
  }
  
  @Test
  def void testProposalFailure() {
    // Commits can only accept concurrent proposals of higher rounds for the same value.
    // Proposals can only be overridden by other proposals of higher rounds.
    /*  1     2     3     4
        1Pa   1Pa   1Pa   2Pb
        1Ua   1Ua   1Ua
        x2Pb              x1Pa
    */
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3005, 4, 1)
    
    val udi = "udi-1"
    val insert = Insert.create(1L, udi, "test", "data-i")
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, "a", 1, 1L)
    val pb2 = Propose.create(4L, udi, insert.record.fingerprint, "b", 1, 2L)
    
    val voteReplies = new HashMap<Integer, Message>
    net.start[ party, reply |
      if (reply.id == 1L && #[1,2,3].contains(party))
        net.send(party, pa1)
      
      if (reply.id == 1L && #[4].contains(party))
        net.send(party, Propose.create(2L, udi, insert.record.fingerprint, "b", 1, 2L))
      
      if (reply.id == 2L && #[1,2,3].contains(party)) {
        waiter.assertVote(reply, 1L)
        val rVote = reply.body as Reply
        
        if (rVote.propose.round == 1) {
          voteReplies.put(rVote.party, reply)
          if (voteReplies.size == 3) {
            val ua1 = Update.create(3L, udi, insert.record.fingerprint, net.quorum.uid, pa1.body as Propose, voteReplies)
            for (sendTo : 1 .. 3)
              net.send(sendTo, ua1)
            voteReplies.clear
          }
        }
      }
      
      if (reply.id == 2L && #[4].contains(party)) {
        waiter.assertVote(reply, 2L)
        net.send(party, Propose.create(3L, udi, insert.record.fingerprint, "b", 1, 1L))
        counter.incrementAndGet
      }
      
      if (reply.id == 3L && #[1].contains(party)) {
        waiter.assertAck(reply)
        net.send(party, pb2)
        counter.incrementAndGet
      }
      
      if (reply.id == 3L && #[4].contains(party)) {
        waiter.assertVote(reply, 2L)
        counter.incrementAndGet
      }
      
      if (reply.id == 4L && #[1].contains(party)) {
        waiter.assertUpdate(reply, insert.record.fingerprint, pa1.body as Propose)
        counter.incrementAndGet
      }
      
      if (counter.get == 4)
        waiter.resume
    ]
    
   for (party : 1 .. 4)
      net.send(party, insert)
    waiter.await(2000)
  }
  
  @Test
  def void testHigherRound() {
    // Commits can accept concurrent proposals of higher rounds for the same value.
    // Proposals can be overridden by commits of the same or higher rounds.
    /*  1     2     3     4
        1Pa   1Pa   1Pa   
                    2Pb   2Pb
        1Ua   1Ua   x1Ua  
        3Pa   3Pa   3Pa
        3Ua   3Ua   3Ua   x3Ua
    */
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3010, 4, 1)
    
    val udi = "udi-1"
    val insert = Insert.create(1L, udi, "test", "data-i")
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, "a", 1, 1L)
    val pb2 = Propose.create(3L, udi, insert.record.fingerprint, "b", 1, 2L)
    val pa3 = Propose.create(5L, udi, insert.record.fingerprint, "a", 1, 3L)
    
    val voteReplies = new HashMap<Integer, Message>
    net.start[ party, reply |
      if (reply.id == 1L && #[1,2,3].contains(party))
        net.send(party, pa1)
      
      if (reply.id == 1L && #[3,4].contains(party))
        net.send(party, pb2)
      
      if (reply.id == 2L && #[1,2,3].contains(party)) {
        waiter.assertVote(reply, 1L)
        val rVote = reply.body as Reply
        
        if (rVote.propose.round == 1) {
          voteReplies.put(rVote.party, reply)
          if (voteReplies.size == 3) {
            val ua1 = Update.create(4L, udi, insert.record.fingerprint, net.quorum.uid, pa1.body as Propose, voteReplies)
            for (sendTo : 1 .. 3)
              net.send(sendTo, ua1)
            voteReplies.clear
          }
        }
      }
      
      if (reply.id == 3L && #[3,4].contains(party)) {
        waiter.assertVote(reply, 2L)
        counter.incrementAndGet
      }
      
      if (reply.id == 4L && #[1,2].contains(party)) {
        waiter.assertAck(reply)
        net.send(party, pa3)
        counter.incrementAndGet
      }
      
      if (reply.id == 4L && #[3].contains(party)) {
        waiter.assertVote(reply, 2L)
        net.send(party, pa3)
        counter.incrementAndGet
      }
      
      if (reply.id == 5L && #[1,2,3].contains(party)) {
        waiter.assertVote(reply, 3L)
        val rVote = reply.body as Reply
        voteReplies.put(rVote.party, reply)
        if (voteReplies.size == 3) {
          val ua2 = Update.create(6L, udi, insert.record.fingerprint, net.quorum.uid, pa3.body as Propose, voteReplies)
          for (sendTo : 1 .. 4)
            net.send(sendTo, ua2)
          voteReplies.clear
        }
      }
      
      if (reply.id == 6L && #[1,2,3].contains(party)) {
        waiter.assertAck(reply)
        counter.incrementAndGet
      }
      
      if (reply.id == 6L && #[4].contains(party)) {
        waiter.assertNoData(reply)
        counter.incrementAndGet
      }
      
      if (counter.get == 9)
        waiter.resume
    ]
    
    for (party : 1 .. 4)
      net.send(party, insert)
    waiter.await(2000)
  }
}
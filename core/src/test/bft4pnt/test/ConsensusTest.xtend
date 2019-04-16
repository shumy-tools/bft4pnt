package bft4pnt.test

import bft4pnt.test.utils.InitQuorum
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import net.jodah.concurrentunit.Waiter
import org.junit.jupiter.api.Test
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update

import static extension bft4pnt.test.utils.WaiterAssertExtensions.*

class ConsensusTest {
  
  @Test
  def void testBasicInsertUpdate() {
    // Proposals can only be overridden by other proposals of higher rounds.
    // An update commit is only valid if it has n -t votes from different parties for the same proposal.
    /*  1              2     3     4
        2Pa            2Pa   2Pa   2Pa
        x2Ua(2-votes)  2Ua   x1Pa
    */
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3000, 4, 1)
    
    val udi = "udi-1"
    val d1 = new Data("d1")
    
    val insert = Insert.create(1L, udi, "test", new Data("data-i"))
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, d1.fingerprint, 1, 2L)
    
    val voteReplies = new ConcurrentHashMap<String, Message>
    net.start([ party, reply |
      if (reply.id == 1L)
        net.send(party, pa1)
      
      if (reply.id == 2L && #[1,2,3,4].contains(party)) {
        waiter.assertVote(reply, 2L)
        
        val rVote = reply.body as Reply
        voteReplies.put(rVote.strParty, reply)
        
        if (voteReplies.size == 2) {
          // update has not enough votes
          val u1 = Update.create(3L, udi, insert.record.fingerprint, 0, pa1.body as Propose, voteReplies, d1)
          net.send(party, u1)
        }
        
        if (voteReplies.size == 3) {
          // should ignore proposals with lower rounds
          val p2 = Propose.create(4L, udi, insert.record.fingerprint, "data-3", 1, 1L)
          net.send(party, p2)
          
          val u2 = Update.create(5L, udi, insert.record.fingerprint, 0, pa1.body as Propose, voteReplies, d1)
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
      
      if (counter.get == 3) {
        ok.set = true
        waiter.resume
      }
      
    ], [
      for (party : 1 .. 4)
        net.send(party, insert)
    ])
    
    waiter.await(4000)
    waiter.assertTrue(ok.get)
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
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3005, 4, 1)
    
    val udi = "udi-1"
    val d1 = new Data("d1")
    val d2 = new Data("d2")
    
    val insert = Insert.create(1L, udi, "test", new Data("data-i"))
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, d1.fingerprint, 1, 1L)
    val pb2 = Propose.create(4L, udi, insert.record.fingerprint, d2.fingerprint, 1, 2L)
    
    val voteReplies = new ConcurrentHashMap<String, Message>
    net.start([ party, reply |
      if (reply.id == 1L && #[1,2,3].contains(party))
        net.send(party, pa1)
      
      if (reply.id == 1L && #[4].contains(party))
        net.send(party, Propose.create(2L, udi, insert.record.fingerprint, "b", 1, 2L))
      
      if (reply.id == 2L && #[1,2,3].contains(party)) {
        waiter.assertVote(reply, 1L)
        val rVote = reply.body as Reply
        
        if (rVote.propose.round == 1) {
          voteReplies.put(rVote.strParty, reply)
          if (voteReplies.size == 3) {
            val ua1 = Update.create(3L, udi, insert.record.fingerprint, 0, pa1.body as Propose, voteReplies, d1)
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
      
      if (counter.get == 4) {
        ok.set = true
        waiter.resume
      }
      
    ], [
      for (party : 1 .. 4)
        net.send(party, insert)
    ])
    
    waiter.await(4000)
    waiter.assertTrue(ok.get)
  }
  
  @Test
  def void testHigherRound() {
    // Proposals can only be overridden by other proposals of higher rounds.
    // Commits can have concurrent proposals of higher rounds for the same value.
    // Proposals can be overridden by commits of the same or higher rounds.
    // Conflicting commits can only be overridden by other commits of higher rounds.
    // On receiving a conflicting commit it should reply with the commit of higher round.
    /*  1     2     3     4
        1Pa   1Pa   1Pa   
                    2Pb   2Pb
        1Ua   1Ua   x1Ua  
        3Pa   3Pa   3Pa
        3Ua   3Ua   3Ua   3Ua
        x1Ua              x1Ua
    */
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3010, 4, 1)
    
    val udi = "udi-1"
    val d1 = new Data("d1")
    val d2 = new Data("d2")
    
    val insert = Insert.create(1L, udi, "test", new Data("data-i"))
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, d1.fingerprint, 1, 1L)
    val pb2 = Propose.create(3L, udi, insert.record.fingerprint, d2.fingerprint, 1, 2L)
    val pa3 = Propose.create(5L, udi, insert.record.fingerprint, d1.fingerprint, 1, 3L)
    val ua1 = new AtomicReference<Message>
    val ua2 = new AtomicReference<Message>
    
    val voteReplies = new ConcurrentHashMap<String, Message>
    net.start([ party, reply |
      if (reply.id == 1L && #[1,2,3].contains(party))
        net.send(party, pa1)
      
      if (reply.id == 1L && #[3,4].contains(party))
        net.send(party, pb2)
      
      if (reply.id == 2L && #[1,2,3].contains(party)) {
        waiter.assertVote(reply, 1L)
        val rVote = reply.body as Reply
        
        if (rVote.propose.round == 1) {
          voteReplies.put(rVote.strParty, reply)
          if (voteReplies.size == 3) {
            ua1.set = Update.create(4L, udi, insert.record.fingerprint, 0, pa1.body as Propose, voteReplies, d1)
            for (sendTo : 1 .. 3)
              net.send(sendTo, ua1.get)
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
        voteReplies.put(rVote.strParty, reply)
        if (voteReplies.size == 3) {
          ua2.set = Update.create(6L, udi, insert.record.fingerprint, 0, pa3.body as Propose, voteReplies, d1)
          for (sendTo : 1 .. 4)
            net.send(sendTo, ua2.get)
          voteReplies.clear
        }
      }
      
      if (reply.id == 6L && #[1,2,3].contains(party)) {
        waiter.assertAck(reply)
        counter.incrementAndGet
      }
      
      if (reply.id == 6L && #[4].contains(party)) {
        waiter.assertAck(reply)
        counter.incrementAndGet
      }
      
      if (counter.get == 9) {
        counter.incrementAndGet
        ua1.get.id = 7L
        net.send(1, ua1.get)
        net.send(4, ua1.get)
      }
      
      if (reply.id == 7L && #[1,4].contains(party)) {
        waiter.assertUpdate(reply, insert.record.fingerprint, pa3.body as Propose)
        counter.incrementAndGet
      }
      
      if (counter.get == 12) {
        ok.set = true
        waiter.resume
      }
      
    ], [
      for (party : 1 .. 4)
        net.send(party, insert)
    ])
    
    waiter.await(4000)
    waiter.assertTrue(ok.get)
  }
  
  @Test
  def void testReplication() {
    // Proposals can be overridden by (n - t) commits from lower rounds, otherwise it should reply with a vote.
    /*  1     2     3     4
        1Pa   1Pa   1Pa   2Pb
        1Ua   1Ua   
    R               1Ua   1Ua(n - t)
    */
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3015, 4, 1)
    
    val udi = "udi-1"
    val d1 = new Data("d1")
    val d2 = new Data("d2")
    
    val insert = Insert.create(1L, udi, "test", new Data("data-i"))
    val pa1 = Propose.create(2L, udi, insert.record.fingerprint, d1.fingerprint, 1, 1L)
    val pb2 = Propose.create(2L, udi, insert.record.fingerprint, d2.fingerprint, 1, 2L)
    
    net.replicator(1).onReplicate = [ quorum, parties, msg |
      if (msg.id > 0L) {
        waiter.assertEquals(insert.record.fingerprint, msg.record.fingerprint)
        waiter.assertEqualSet(parties, #{net.getPartyAtIndex(2), net.getPartyAtIndex(3), net.getPartyAtIndex(4)})
        counter.incrementAndGet
        println('''ON-REPLICATE: (q=«quorum», to=«parties», msg=«msg») -> «counter.get»''')
      }
    ]
    
    net.replicator(1).onReply = [
      if (strParty == net.getPartyAtIndex(4) && type == Reply.Type.VOTE)
        counter.incrementAndGet
      
      counter.incrementAndGet
      println('''ON-REPLY: (party=«strParty», type=«type», update=«propose !== null») -> «counter.get»''')
      if (counter.get == 18) {
        println('''----------------START-REP 2----------------''')
        net.replicator(2).replicate
      }
    ]
    
    net.replicator(2).onReplicate = [ quorum, parties, msg |
      if (msg.id > 0L) {
        waiter.assertEquals(insert.record.fingerprint, msg.record.fingerprint)
        waiter.assertEqualSet(parties, #{net.getPartyAtIndex(3), net.getPartyAtIndex(4)})
        counter.incrementAndGet
        println('''ON-REPLICATE: (q=«quorum», to=«parties», msg=«msg») -> «counter.get»''')
      }
    ]
    
    net.replicator(2).onReply = [
      counter.incrementAndGet
      println('''ON-REPLY: (party=«strParty», type=«type», update=«propose !== null») -> «counter.get»''')
      if (counter.get == 28) {
        println('''----------------START-REP 3----------------''')
        net.replicator(3).replicate
      }
    ]
    
    net.replicator(3).onReplicate = [ quorum, parties, msg |
      if (msg.id > 0L) {
        waiter.assertEquals(insert.record.fingerprint, msg.record.fingerprint)
        waiter.assertEqualSet(parties, #{net.getPartyAtIndex(4)})
        counter.incrementAndGet
        println('''ON-REPLICATE: (q=«quorum», to=«parties», msg=«msg») -> «counter.get»''')
      }
    ]
    
    net.replicator(3).onReply = [
      waiter.assertTrue(type != Reply.Type.VOTE)
      
      counter.incrementAndGet
      println('''ON-REPLY: (party=«strParty», type=«type», update=«propose !== null») -> «counter.get»''')
      if (counter.get == 34) {
        ok.set = true
        waiter.resume
      }
    ]
    
    val updateReplies = new AtomicInteger(0)
    val voteReplies = new ConcurrentHashMap<String, Message>
    net.start([ party, reply |
      if (reply.id == 1L && #[1,2,3].contains(party))
        net.send(party, pa1)
      
      if (reply.id == 1L && #[4].contains(party))
        net.send(party, pb2)
      
      if (reply.id == 2L && #[1,2,3].contains(party)) {
        waiter.assertVote(reply, 1L)
        val rVote = reply.body as Reply
        
        voteReplies.put(rVote.strParty, reply)
        if (voteReplies.size == 3) {
          val ua1 = Update.create(3L, udi, insert.record.fingerprint, 0, pa1.body as Propose, voteReplies, d1)
          for (sendTo : 1 .. 2)
            net.send(sendTo, ua1)
          voteReplies.clear
        }
      }
      
      if (reply.id == 2L && #[4].contains(party)) {
        waiter.assertVote(reply, 2L)
        counter.incrementAndGet
      }
      
      if (reply.id == 3L && #[1,2].contains(party)) {
        waiter.assertAck(reply)
        counter.incrementAndGet
        
        if (updateReplies.incrementAndGet == 2) {
          println('''----------------START-REP 1----------------''')
          net.replicator(1).replicate
        }
      }
    ], [
      for (party : 1 .. 4)
        net.send(party, insert)
    ])
    
    waiter.await(4000)
    waiter.assertTrue(ok.get)
  }
  
}
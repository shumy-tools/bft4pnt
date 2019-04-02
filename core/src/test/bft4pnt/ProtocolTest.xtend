package bft4pnt

import java.util.HashMap
import net.jodah.concurrentunit.Waiter
import org.junit.jupiter.api.Test
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.QuorumConfig
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update

class ProtocolTest {
  
  @Test
  def void testInsertUpdate() {
    val waiter = new Waiter
    val quorum = new QuorumConfig(7, 1)
    val net  = InitQuorum.init(quorum)
    
    val udi = "udi-1"
    val insert = Insert.create(1L, udi, "test", "data-1")
    val p1 = Propose.create(2L, udi, insert.record.fingerprint, "data-2", 1, 2)
    
    val voteReplies = new HashMap<Integer, Message>
    net.start[ party, reply |
      if (reply.id === 1) {
        net.send(party, p1)
      }
      
      if (reply.id === 2) {
        val rVote = reply.body as Reply
        voteReplies.put(rVote.party, reply)
        
        if (voteReplies.size === 6) {
          // should ignore lower rounds
          val p2 = Propose.create(3L, udi, insert.record.fingerprint, "data-3", 1, 1)
          net.send(party, p2)
          
          val u1 = Update.create(4L, udi, insert.record.fingerprint, quorum, p1.body as Propose, voteReplies)
          net.send(party, u1)
        }
      }
      
      if (reply.id === 3) {
        waiter.assertTrue(reply.body instanceof Reply)
        waiter.assertEquals((reply.body as Reply).type, Reply.Type.VOTE)
        waiter.assertEquals((reply.body as Reply).propose.round, 2L)
      }
      
      if (reply.id === 4) {
        waiter.assertTrue(reply.body instanceof Reply)
        waiter.assertEquals((reply.body as Reply).type, Reply.Type.ACK)
        waiter.resume
      }
    ]
    
    for (party : 1 .. 7)
      net.send(party, insert)
    waiter.await(2000)
  }
}
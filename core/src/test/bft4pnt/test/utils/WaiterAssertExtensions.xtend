package bft4pnt.test.utils

import java.util.Set
import net.jodah.concurrentunit.Waiter
import pt.ieeta.bft4pnt.msg.Error
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Propose
import pt.ieeta.bft4pnt.msg.Reply
import pt.ieeta.bft4pnt.msg.Update

class WaiterAssertExtensions {
  static def void assertEqualSet(Waiter waiter, Set<String> one, Set<String> two) {
    waiter.assertTrue(one.containsAll(two))
    waiter.assertTrue(two.containsAll(one))
  }
  
  static def void assertError(Waiter waiter, Message reply, String msg) {
    waiter.assertTrue(reply.body instanceof Error)
    waiter.assertEquals(msg, (reply.body as Error).msg)
  }
  
  static def void assertAck(Waiter waiter, Message reply) {
    waiter.assertTrue(reply.body instanceof Reply)
    waiter.assertEquals(Reply.Type.ACK, (reply.body as Reply).type)
  }
  
  static def void assertVote(Waiter waiter, Message reply, long round) {
    waiter.assertTrue(reply.body instanceof Reply)
    waiter.assertEquals(Reply.Type.VOTE, (reply.body as Reply).type)
    waiter.assertEquals(round, (reply.body as Reply).propose.round)
  }
  
  static def void assertInsert(Waiter waiter, Message reply, String rec, String type) {
    waiter.assertTrue(reply.body instanceof Insert)
    waiter.assertEquals(rec, reply.record.fingerprint)
    val insert = reply.body as Insert
    
    waiter.assertEquals(type, insert.type)
  }
  
  static def void assertUpdate(Waiter waiter, Message reply, String rec, Propose propose) {
    waiter.assertTrue(reply.body instanceof Update)
    waiter.assertEquals(rec, reply.record.fingerprint)
    val update = reply.body as Update
    
    waiter.assertEquals(propose.index, update.propose.index)
    waiter.assertEquals(propose.fingerprint, update.propose.fingerprint)
    waiter.assertEquals(propose.round, update.propose.round)
  }
  
  static def void assertNoData(Waiter waiter, Message reply) {
    waiter.assertTrue(reply.body instanceof Reply)
    waiter.assertEquals(Reply.Type.NO_DATA, (reply.body as Reply).type)
  }
}
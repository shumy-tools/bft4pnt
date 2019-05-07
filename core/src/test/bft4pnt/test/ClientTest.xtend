package bft4pnt.test

import bft4pnt.test.utils.InitQuorum
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import net.jodah.concurrentunit.Waiter
import org.junit.jupiter.api.Test
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Insert

class ClientTest {
  @Test
  def void testClientUploads() {
    val udi = "udi-1"
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(3000, 4, 1)
    val channels = net.createDataChannels("/home/micael/data/screen-data")
    
    val insert = Insert.create(1L, udi, "test", new Data("data-i"))
    
    net.start([ party, reply |
      counter.incrementAndGet
      if (counter.get === 4) {
        net.client.get(insert.record.fingerprint)
        //ok.set = true
        //waiter.resume
      }
      
    ],[
      for (party : 1 .. 4)
        net.send(party, insert)
    ])
    
    waiter.await(400000)
    waiter.assertTrue(ok.get)
  }
}
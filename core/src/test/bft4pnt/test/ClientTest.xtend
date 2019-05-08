package bft4pnt.test

import bft4pnt.test.utils.InitQuorum
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import java.io.File
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import net.jodah.concurrentunit.Waiter
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.msg.Data
import pt.ieeta.bft4pnt.msg.Insert

class ClientTest {
  
  // stores will be set at ~/test-data
  private def File genFile(int size) {
    val byteSize = size * 1024 * 1024
    val home = System.getProperty("user.home")
    val original = new File(home + "/test-data/" + UUID.randomUUID.toString)
    
    val writer = new PrintWriter(original, "UTF-8")
    var writtenBytes = 0
    do {
      val randomWords = UUID.randomUUID.toString
      writtenBytes += randomWords.getBytes(StandardCharsets.UTF_8).length
      writer.print(randomWords)
    } while(writtenBytes < byteSize)
    
    val fName = new Data(original).fingerprint.replace("/", "_").replace("+", "-")
    val file = new File(home + "/test-data/" + fName + "-0")
    
    original.renameTo(file)
    
    println('''FILE ( «size»MB ) : «file»''')
    return file
  } 
  
  @Test
  def void testClientRetrieveTimes() {
    val root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
    root.level = Level.ERROR
    
    val home = System.getProperty("user.home")
    new File(home + "/test-data").mkdirs
    
    //runTest(8, 2)
    
    for (i: 0 .. 3) {
      runTest(4, i)
    }
  }
  
  def void runTest(int parties, int i) {
    val home = System.getProperty("user.home")
    
    val size = 100 * Math.pow(2, i) as int
    val file = genFile(size)
    testClientRetrieve(parties, 3000 + 10*i, file)
    testClientSliceRetrieve(parties, 4000 + 10*i, file)
    
    Files.walk(Paths.get(home + "/test-data"))
              .filter[Files.isRegularFile(it)]
              .forEach[ Files.delete(it) ]
  }
  
  def void testClientRetrieve(int parties, int port, File file) {
    val udi = "udi-1"
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(port, parties, 1)
    net.createDataChannels
    
    val insert = Insert.create(1L, udi, "test", new Data(file))
    
    val time = new AtomicLong(0L)
    net.start([ party, reply |
      counter.incrementAndGet
      if (counter.get === parties) {
        time.set = System.currentTimeMillis
        net.client.get(insert.record.fingerprint).thenAccept[
          ok.set = true
          waiter.resume
        ]
      }
      
    ],[
      for (party : 1 .. parties)
        net.send(party, insert)
    ])
    
    waiter.await(400000)
    waiter.assertTrue(ok.get)
    val delta = (System.currentTimeMillis - time.get) / 1000.0
    println('''  SIMPLE-RETRIEVE IN «delta»s''')
    net.stop
  }
  
  def void testClientSliceRetrieve(int parties, int port, File file) {
    val udi = "udi-1"
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(port, parties, 1)
    net.createDataChannels
    
    val insert = Insert.create(1L, udi, "test", new Data(file), 4)
    
    val time = new AtomicLong(0L)
    net.start([ party, reply |
      counter.incrementAndGet
      if (counter.get === parties) {
        time.set = System.currentTimeMillis
        net.client.get(insert.record.fingerprint).thenAccept[
          ok.set = true
          waiter.resume
        ]
      }
    ],[
      for (party : 1 .. parties)
        net.send(party, insert)
    ])
    
    waiter.await(400000)
    waiter.assertTrue(ok.get)
    val delta = (System.currentTimeMillis - time.get) / 1000.0
    println('''  SLICE-RETRIEVE IN «delta»s''')
    net.stop
  }
  
}
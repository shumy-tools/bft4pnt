package bft4pnt.test

import bft4pnt.test.utils.InitQuorum
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream
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

import static java.lang.System.*

class ClientTest {
  def outputFile(String name) {
    new PrintStream(new BufferedOutputStream(new FileOutputStream(name)), true)
  }
  
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
    
    //println('''FILE ( «size»MB ) : «file»''')
    return file
  } 
  
  @Test
  def void testClientRetrieveTimes() {
    try {
      System.setOut = outputFile("eval.txt")
      System.setErr = outputFile("error.txt")
      
      val eval = System.getenv("EVAL")
      if (eval === null || !Boolean.parseBoolean(eval))
        return;
      
      val $1 = System.getenv("PARTIES")
      val parties = Integer.parseInt($1)
      
      val $2 = System.getenv("SIZE")
      val size = Integer.parseInt($2)
      
      val root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
      root.level = Level.ERROR
      
      val home = System.getProperty("user.home")
      new File(home + "/test-data").mkdirs
      
      println('''Eval retrieves -> (parties=«parties», size=«size»MB)''')
      
      val file = genFile(size)
      testClientRetrieve(parties, 5000, file)
      testClientSliceRetrieve(parties, 6000, file)
      
      Files.walk(Paths.get(home + "/test-data"))
              .filter[Files.isRegularFile(it)]
              .forEach[ Files.delete(it) ]
              
    } catch (Throwable ex) {
      ex.printStackTrace
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
    println('''  SIMPLE IN «delta»s''')
    net.stop
  }
  
  def void testClientSliceRetrieve(int parties, int port, File file) {
    val udi = "udi-1"
    
    val ok = new AtomicBoolean(false)
    val waiter = new Waiter
    val counter = new AtomicInteger(0)
    val net  = InitQuorum.init(port, parties, 1)
    net.createDataChannels
    
    val insert = Insert.create(1L, udi, "test", new Data(file), parties)
    
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
    println('''  SLICED IN «delta»s''')
    net.stop
  }
  
}
import java.net.InetSocketAddress
import java.security.Security
import org.bouncycastle.jce.provider.BouncyCastleProvider
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Record
import pt.ieeta.bft4pnt.PNTServer
import pt.ieeta.bft4pnt.store.MemoryStore
import pt.ieeta.bft4pnt.msg.QuorumConfig
import java.util.ArrayList
import java.security.PublicKey
import java.util.HashMap
import pt.ieeta.bft4pnt.msg.Error

class MainTest {
  def static void main(String[] args) {
    Security.addProvider(new BouncyCastleProvider)
    
    val addresses = new HashMap<String, PublicKey>
    val (InetSocketAddress)=>PublicKey resolver = [
      val address = '''«hostString»:«port»'''
      val key = addresses.get(address)
      if (key === null)
        throw new RuntimeException('''No key for address: «address»''')
      
      key
    ]
    
    val parties = new ArrayList<InetSocketAddress>
    for (i : 0..6) {
      val inet = new InetSocketAddress("127.0.0.1", 3001 + i)
      val keys = KeyPairHelper.genKeyPair
      addresses.put('''«inet.hostString»:«inet.port»''', keys.public)
      
      val broker = new MessageBroker(inet, keys.private, resolver)
      val store = new MemoryStore(new QuorumConfig(7, 1))
      new PNTServer(broker, store).start
      parties.add(inet)
    }
    
    Thread.sleep(1000)
    
    val inet = new InetSocketAddress("127.0.0.1", 3000)
    val keys = KeyPairHelper.genKeyPair
    addresses.put('''«inet.hostString»:«inet.port»''', keys.public)
    
    val client = new MessageBroker(inet, keys.private, resolver)
    
    val record = new Record("client-1", "finger-123")
    val body = new Insert("DICOM")
    val msg = new Message(record, body) => [
      id = 1L
      address = parties.get(0)
    ]
    
    client.start[ reply |
      if (reply.type === Message.Type.ERROR) {
        val error = reply.body as Error
        println(error.msg)
      } else
        println("OK")
    ]
    
    client.send(msg)
  }
}
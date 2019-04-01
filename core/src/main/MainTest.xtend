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
    
    val partyKeys = new HashMap<Integer, PublicKey>
    val (Integer)=>PublicKey resolver = [
      val key = partyKeys.get(it)
      if (key === null)
        throw new RuntimeException('''No key for party: «it»''')
      key
    ]
    
    val parties = new ArrayList<InetSocketAddress>
    for (i : 0..6) {
      val inet = new InetSocketAddress("127.0.0.1", 3001 + i)
      val keys = KeyPairHelper.genKeyPair
      partyKeys.put(i + 1, keys.public)
      
      val broker = new MessageBroker(inet, keys)
      val store = new MemoryStore(new QuorumConfig(7, 1))
      new PNTServer(broker, store, resolver).start
      parties.add(inet)
    }
    
    Thread.sleep(1000)
    
    val inet = new InetSocketAddress("127.0.0.1", 3000)
    val keys = KeyPairHelper.genKeyPair
    val client = new MessageBroker(inet, keys)
    
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
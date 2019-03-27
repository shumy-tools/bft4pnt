import java.net.InetSocketAddress
import java.security.Security
import org.bouncycastle.jce.provider.BouncyCastleProvider
import pt.ieeta.bft4pnt.broker.MessageBroker
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Record

class MainTest {
  def static void main(String[] args) {
    Security.addProvider(new BouncyCastleProvider)
    
    val keys = KeyPairHelper.genKeyPair
    val inetAddress = new InetSocketAddress("localhost", 3009)
    val broker = new MessageBroker(inetAddress, keys.private, keys.public)
    
    broker.start[
      println("OK")
    ]
    
    Thread.sleep(1000)

    val record = new Record("udi-1234", "finger-123")
    val body = new Insert("DICOM")
    val msg = new Message(record, body) => [
      id = 1L
      address = inetAddress
    ]
    
    broker.send(msg)
  }
}
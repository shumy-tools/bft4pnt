import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Record
import pt.ieeta.bft4pnt.msg.Insert
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import java.security.Security

class MainTest {
  def static void main(String[] args) {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider)
    
    val keys = KeyPairHelper.genKeyPair
    
    val record = new Record("udi-1234", "finger-123")
    val body = new Insert("DICOM")
    val msg = new Message(record, body)
    
    val block = msg.write(keys.private)
    val result = Message.read(block, keys.public)
    if (result.hasError)
      println(result.error.msg)
    else
      println('OK')
  }
}
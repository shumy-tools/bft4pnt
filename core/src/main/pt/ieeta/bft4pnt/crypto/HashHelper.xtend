package pt.ieeta.bft4pnt.crypto

import java.security.MessageDigest
import org.bouncycastle.util.encoders.Base64
import java.nio.charset.StandardCharsets

class HashHelper {
  static def String digest(String data) {
    digest(data.getBytes(StandardCharsets.UTF_8))
  }
  
  static def String digest(byte[] data) {
    val digest = MessageDigest.getInstance("SHA-256")
    return new String(Base64.encode(digest.digest(data)), StandardCharsets.UTF_8)
  }
}
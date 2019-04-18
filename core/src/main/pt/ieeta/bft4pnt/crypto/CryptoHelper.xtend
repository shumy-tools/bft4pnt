package pt.ieeta.bft4pnt.crypto

import java.nio.charset.StandardCharsets
import java.security.KeyFactory
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.PrivateKey
import java.security.PublicKey
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import org.bouncycastle.util.encoders.Base64

class CryptoHelper {
  public static val String PROVIDER = "BC"
  public static val String KEY_ALG = "Ed25519"//"DSA"
  public static val String SIG_ALG = "Ed25519"//"SHA256withDSA"
  
  static val keyFactory = KeyFactory.getInstance(KEY_ALG, PROVIDER)
  
  def static KeyPair genKeyPair() {
    val kpg = KeyPairGenerator.getInstance(KEY_ALG, PROVIDER)
    val kp = kpg.generateKeyPair
    return kp
  }
  
  def static PublicKey read(byte[] encKey) {
    val pubKeySpec = new X509EncodedKeySpec(encKey)
    return keyFactory.generatePublic(pubKeySpec)
  }
  
  def static String encode(PublicKey key) {
    new String(Base64.encode(key.encoded), StandardCharsets.UTF_8)
  }
  
  def static String encode(PrivateKey key) {
    new String(Base64.encode(key.encoded), StandardCharsets.UTF_8)
  }
  
  def static PrivateKey decodePrivateKey(String key) {
    val encKey = Base64.decode(key.getBytes(StandardCharsets.UTF_8))
    val prvKeySpec = new PKCS8EncodedKeySpec(encKey)
    return keyFactory.generatePrivate(prvKeySpec)
  }
}
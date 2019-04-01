package pt.ieeta.bft4pnt.crypto

import java.nio.charset.StandardCharsets
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.PrivateKey
import java.security.PublicKey
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.jcajce.provider.asymmetric.edec.BCEdDSAPrivateKey
import org.bouncycastle.jcajce.provider.asymmetric.edec.BCEdDSAPublicKey
import org.bouncycastle.util.encoders.Base64

class KeyPairHelper {
  static val pubConst = BCEdDSAPublicKey.getDeclaredConstructor(SubjectPublicKeyInfo) => [
    accessible = true
  ]
  
  static val prvConst = BCEdDSAPrivateKey.getDeclaredConstructor(PrivateKeyInfo) => [
    accessible = true
  ]

  def static KeyPair genKeyPair() {
    val kpg = KeyPairGenerator.getInstance("Ed25519", "BC")
    val kp = kpg.generateKeyPair
    return kp
  }
  
  def static PublicKey read(byte[] key) {
    val spki = SubjectPublicKeyInfo.getInstance(key)
    return pubConst.newInstance(spki)
  }
  
  def static PublicKey decodePublicKey(String key) {
    val data = Base64.decode(key.getBytes(StandardCharsets.UTF_8))
    val spki = SubjectPublicKeyInfo.getInstance(data)
    return pubConst.newInstance(spki)
  }
  
  def static PrivateKey decodePrivateKey(String key) {
    val data = Base64.decode(key.getBytes(StandardCharsets.UTF_8))
    val pki = PrivateKeyInfo.getInstance(data)
    return prvConst.newInstance(pki)
  }
  
  def static String encode(PublicKey key) {
    val pubKey = key as BCEdDSAPublicKey
    return new String(Base64.encode(pubKey.encoded), StandardCharsets.UTF_8)
  }
  
  def static String encode(PrivateKey key) {
    val prvKey = key as BCEdDSAPrivateKey
    return new String(Base64.encode(prvKey.encoded), StandardCharsets.UTF_8)
  }
}
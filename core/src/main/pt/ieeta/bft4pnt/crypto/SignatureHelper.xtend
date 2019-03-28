package pt.ieeta.bft4pnt.crypto

import java.security.PrivateKey
import java.security.PublicKey
import java.security.Signature

class SignatureHelper {
  public static val String algorithm = "Ed25519"
  
  static def byte[] sign(PrivateKey prvKey, ArraySlice slice) {
    val signer = Signature.getInstance(algorithm, "BC") => [
      initSign(prvKey)
      update(slice.data, slice.offset, slice.length - slice.offset)
    ]
    
    return signer.sign
  }
  
  static def boolean verify(PublicKey pubKey, ArraySlice slice, byte[] signature) {
    val verifier = Signature.getInstance(algorithm, "BC") => [
      initVerify(pubKey)
      update(slice.data, slice.offset, slice.length - slice.offset - signature.length - 4) //also remove the int field for the signature size
    ]
    
    return verifier.verify(signature)
  }
}
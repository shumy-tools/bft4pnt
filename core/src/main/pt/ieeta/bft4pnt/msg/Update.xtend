package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.util.ArrayList
import java.util.List
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import java.security.PublicKey
import pt.ieeta.bft4pnt.crypto.KeyPairHelper
import org.bouncycastle.util.encoders.Base64
import java.nio.charset.StandardCharsets

@FinalFieldsConstructor
class Vote implements ISection {
  public val String party
  public val PublicKey key
  public val byte[] signature
  
  override write(ByteBuf buf) {
    val bKey = KeyPairHelper.write(key)
    Message.writeBytes(buf, bKey)
    Message.writeBytes(buf, signature)
  }
  
  static def Vote read(ByteBuf buf) {
    val bKey = Message.readBytes(buf)
    val key = KeyPairHelper.read(bKey)
    
    val signature = Message.readBytes(buf)
    
    val party = new String(Base64.encode(bKey), StandardCharsets.UTF_8)
    return new Vote(party, key, signature)
  }
}

@FinalFieldsConstructor
class Update implements ISection {
  public val QuorumConfig quorum
  public val Propose propose
  
  public val List<Vote> votes
  public val Slices slices
  
  new(QuorumConfig quorum, Propose propose, List<Vote> votes) {
    this.quorum = quorum
    this.propose = propose
    
    this.votes = votes
    this.slices = new Slices
  }
  
  override write(ByteBuf buf) {
    quorum.write(buf)
    propose.write(buf)
    
    buf.writeInt(votes.size)
    for (vote : votes)
      vote.write(buf)
    
    slices.write(buf)
  }
  
  static def Update read(ByteBuf buf) {
    val quorum = QuorumConfig.read(buf)
    val propose = Propose.read(buf)
    
    val number = buf.readInt
    val votes = new ArrayList<Vote>(number)
    for (n : 0..number) {
      val vote = Vote.read(buf)
      votes.add(vote)
    }
    
    val slices = Slices.read(buf)
    
    return new Update(quorum, propose, votes, slices)
  }
}
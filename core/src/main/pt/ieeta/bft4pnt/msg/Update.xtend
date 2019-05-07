package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.util.ArrayList
import java.util.List
import java.util.Map
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.eclipse.xtend.lib.annotations.Accessors

@FinalFieldsConstructor
class Update implements ISection, HasSlices {
  public val Integer quorum
  public val Propose propose
  
  public val List<Signature> votes
  @Accessors val Slices slices
  
  new(Integer quorum, Propose propose, List<Signature> votes) {
    this.quorum = quorum
    this.propose = propose
    
    this.votes = votes
    this.slices = new Slices
  }
  
  override write(ByteBuf buf) {
    buf.writeInt(quorum)
    propose.write(buf)
    
    buf.writeInt(votes.size)
    for (vote : votes)
      vote.write(buf)
    
    slices.write(buf)
  }
  
  static def Update read(ByteBuf buf) {
    val quorum = buf.readInt
    val propose = Propose.read(buf)
    
    val number = buf.readInt
    val votes = new ArrayList<Signature>(number)
    for (n : 0 ..< number) {
      val vote = Signature.read(buf)
      votes.add(vote)
    }
    
    val slices = Slices.read(buf)
    
    return new Update(quorum, propose, votes, slices)
  }
   
  static def create(long msgId, String udi, String rec, Integer quorum, Propose propose, Map<String, Message> voteReplies, Data block) {
    val votes = new ArrayList<Signature>
    for (party : voteReplies.keySet) {
      val msgReply = voteReplies.get(party)
      val reply = msgReply.body
      if (reply instanceof Reply) {
        if (reply.type === Reply.Type.VOTE
          && reply.propose.index === propose.index && reply.propose.fingerprint == propose.fingerprint && reply.propose.round === propose.round
        ) {
          val vote = new Signature(reply.party, msgReply.signature)
          votes.add(vote)
        }
      }
    }
    
    val record = new Record(udi, rec)
    val body = new Update(quorum, propose, votes)
    
    return new Message(record, body) => [
      id = msgId
      data = block
    ]
  }
}
package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.util.ArrayList
import java.util.List
import java.util.Map
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Vote implements ISection {
  public val Integer party
  public val byte[] signature
  
  override write(ByteBuf buf) {
    buf.writeInt(party)
    Message.writeBytes(buf, signature)
  }
  
  static def Vote read(ByteBuf buf) {
    val party = buf.readInt
    val signature = Message.readBytes(buf)
    
    return new Vote(party, signature)
  }
}

@FinalFieldsConstructor
class Update implements ISection {
  public val Integer quorum
  public val Propose propose
  
  public val List<Vote> votes
  public val Slices slices
  
  new(Integer quorum, Propose propose, List<Vote> votes) {
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
    val votes = new ArrayList<Vote>(number)
    for (n : 0 ..< number) {
      val vote = Vote.read(buf)
      votes.add(vote)
    }
    
    val slices = Slices.read(buf)
    
    return new Update(quorum, propose, votes, slices)
  }
   
  static def create(long msgId, String udi, String rec, Integer quorum, Propose propose, Map<Integer, Message> voteReplies) {
    val votes = new ArrayList<Vote>
    for (party : voteReplies.keySet) {
      val msgReply = voteReplies.get(party)
      val reply = msgReply.body
      if (reply instanceof Reply) {
        if (reply.type === Reply.Type.VOTE
          && reply.quorum !== null && reply.quorum == quorum
          && reply.propose !== null && reply.propose.index === propose.index && reply.propose.fingerprint == propose.fingerprint && reply.propose.round === propose.round
        ) {
          val vote = new Vote(party, msgReply.signature)
          votes.add(vote) 
        }
      }
    }
    
    val record = new Record(udi, rec)
    val body = new Update(quorum, propose, votes)
    
    return new Message(record, body) => [
      id = msgId
      data = new Data(propose.type, propose.data)
    ]
  }
}
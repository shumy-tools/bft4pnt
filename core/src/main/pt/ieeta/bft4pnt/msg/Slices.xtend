package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.util.ArrayList
import java.util.List
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Slices implements ISection {
  public val int size               //in MB, 0 if there are no slices
  public val List<String> slices    //fingerprints of slices
  
  new() {
    this.size = 0
    this.slices = null
  }
  
  override write(ByteBuf buf) {
    buf.writeInt(size)
    
    if (size !== 0) {
      buf.writeInt(slices.size)
      for (slice : slices)
        Message.writeString(buf, slice)
    }
  }
  
  static def Slices read(ByteBuf buf) {
    val size = buf.readInt
    
    if (size !== 0) {
      val number = buf.readInt
      val slices = new ArrayList<String>(number)
      for (n : 0 ..< number)
        slices.add(Message.readString(buf))
        
      return new Slices(size, slices)
    }
    
    return new Slices
  }
}
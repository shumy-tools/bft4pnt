package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import java.util.ArrayList
import java.util.Collections
import java.util.List
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class Get implements ISection {
  public val int index
  public val List<Integer> slices
  
  static def Get get() {
    return new Get(-1, Collections.EMPTY_LIST)
  }
  
  static def Get get(int index) {
    return new Get(index, Collections.EMPTY_LIST)
  }
  
  static def Get retrieve(int index, List<Integer> slices) {
    return new Get(index, slices)
  }
  
  override write(ByteBuf buf) {
    buf.writeInt(index)
    
    buf.writeInt(slices.size)
    for (slice : slices)
      buf.writeInt(slice)
  }
  
  static def Get read(ByteBuf buf) {
    val index = buf.readInt
    
    val number = buf.readInt
    val slices = new ArrayList<Integer>(number)
    for (n : 0..number)
      slices.add(buf.readInt)
    
    return new Get(index, slices)
  }
}
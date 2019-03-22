package pt.ieeta.bft4pnt.msg

import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import io.netty.buffer.ByteBuf

@FinalFieldsConstructor
class Error implements ISection {
  enum Code { INTERNAL, UNAUTHORIZED, INVALID, CONSTRAINT }
  
  public val Code code
  public val String msg
  
  override write(ByteBuf buf) {
    buf.writeShort(code.ordinal)
    Message.writeString(buf, msg)
  }
  
  static def Error read(ByteBuf buf) {
    val codeIndex = buf.readShort as int
    val code = Code.values.get(codeIndex)
    val msg = Message.readString(buf)
    
    return new Error(code, msg)
  }
}
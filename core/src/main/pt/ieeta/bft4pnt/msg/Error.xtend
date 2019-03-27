package pt.ieeta.bft4pnt.msg

import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import io.netty.buffer.ByteBuf

@FinalFieldsConstructor
class Error implements ISection {
  enum Code { INTERNAL, UNAUTHORIZED, INVALID, CONSTRAINT }
  
  public val Code code
  public val String msg
  
  static def Error internal(String msg) {
    return new Error(Code.INTERNAL, msg)
  }
  
  static def Error unauthorized(String msg) {
    return new Error(Code.UNAUTHORIZED, msg)
  }

  static def Error invalid(String msg) {
    return new Error(Code.INVALID, msg)
  }
  
  static def Error constraint(String msg) {
    return new Error(Code.CONSTRAINT, msg)
  }
  
  def toMessage() {
    val record = new Record("no-u", "no-f")
    return new Message(record, this)
  }
  
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
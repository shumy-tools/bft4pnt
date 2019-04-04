package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.DigestHelper

@FinalFieldsConstructor
class Data implements ISection {
  enum Type { RAW, STRING, SECTION }
  enum Status { YES, NO, PENDING }
  
  public val Type type
  
  val Object obj
  val String secType
  
  new(byte[] value) { this(Type.RAW, value, null) }
  new(String value) { this(Type.STRING, value, null) }
  new(ISection value) { this(Type.SECTION, value, value.class.name) }
  
  def has(String key) {
    //TODO: change method for big data!
    return Status.YES
  }
  
  def byte[] getRaw() { obj as byte[] }
  
  def getString() {
    if (type !== Type.STRING)
      throw new RuntimeException('''Wrong data type retrieve! (type=«type», try=«Type.STRING»)''')
    
    return obj as String
  }
  
  def <T extends ISection> T get(Class<T> clazz) {
    if (type !== Type.SECTION || secType != clazz.name)
      throw new RuntimeException('''Wrong section type retrieve! (type=«type», secType=«secType», try=(«Type.SECTION», «clazz.name»))''')
    
    return obj as T
  }
  
  def fingerprint() {
    switch type {
      case RAW: DigestHelper.digest(obj as byte[])
      case STRING: DigestHelper.digest(obj as String)
      case SECTION: DigestHelper.digest(obj as ISection)
    }
  }
  
  def verify(String key, Slices slices) {
    //TODO: verify slices?
    return true
  }
  
  override write(ByteBuf buf) {
    buf.writeShort(type.ordinal)
    
    switch type {
      case RAW: Message.writeBytes(buf, obj as byte[])
      case STRING: Message.writeString(buf, obj as String)
      case SECTION: {
        Message.writeString(buf, secType)
        (obj as ISection).write(buf)
      }
    }
  }
  
  static def Data read(ByteBuf buf) {
    val typeIndex = buf.readShort as int
    val type = Type.values.get(typeIndex)
    
    switch type {
      case RAW: {
        val obj = Message.readBytes(buf)
        new Data(obj)
      }
      
      case STRING: {
        val obj = Message.readString(buf)
        new Data(obj)
      }
      
      case SECTION: {
        val secType = Message.readString(buf)
        val clazz = Class.forName(secType)
        val meth = clazz.getDeclaredMethod("read", ByteBuf)
        val obj = meth.invoke(clazz, buf) as ISection
        new Data(obj)
      }
    }
  }
}
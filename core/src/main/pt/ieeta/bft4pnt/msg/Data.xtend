package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import java.io.File
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import pt.ieeta.bft4pnt.crypto.DigestHelper

@FinalFieldsConstructor
class Data implements ISection {
  enum Type { EMPTY, RAW, INTEGER, STRING, SECTION, FILE }
  
  public val Type type
  
  val Object obj
  val String secType
  
  var String fingerprint = null
  
  new() { this(Type.EMPTY, null, null) }
  
  new(byte[] value) { this(Type.RAW, value, null) }
  new(Integer value) { this(Type.INTEGER, value, null) }
  new(String value) { this(Type.STRING, value, null) }
  new(ISection value) { this(Type.SECTION, value, value.class.name) }
  new(File value) { this(Type.FILE, value, value.name) }
  
  def byte[] getRaw() {
    if (type !== Type.RAW)
      throw new RuntimeException('''Wrong data type retrieve! (type=«type», try=«Type.STRING»)''')
    //TODO: get bytes from other types!
    obj as byte[]
  }
  
  def getInteger() {
    if (type !== Type.INTEGER)
      throw new RuntimeException('''Wrong data type retrieve! (type=«type», try=«Type.INTEGER»)''')
    
    return obj as Integer
  }
  
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
    if (fingerprint !== null)
      return fingerprint
    
    this.fingerprint = switch type {
      case EMPTY: {} //do nothing
      case RAW: DigestHelper.digest(obj as byte[])
      case INTEGER: {
        val buf = PooledByteBufAllocator.DEFAULT.buffer(1024).writeInt(obj as Integer)
        try {
          DigestHelper.digest(buf)
        } finally {
          buf.release
        }
      }
      case STRING: DigestHelper.digest(obj as String)
      case SECTION: DigestHelper.digest(obj as ISection)
      case FILE: DigestHelper.digest(obj as File)
    }
  }
  
  def sliceFingerprint(int nSlices) {
    if (type == Type.FILE) {
      val file = obj as File
      return DigestHelper.digestSlices(file, nSlices)
    }
    
    // No slices if not a file!
    return new Slices
  }
  
  override write(ByteBuf buf) {
    buf.writeShort(type.ordinal)
    
    switch type {
      case EMPTY: {} //do nothing
      case RAW: Message.writeBytes(buf, obj as byte[])
      case INTEGER: buf.writeInt(obj as Integer)
      case STRING: Message.writeString(buf, obj as String)
      case SECTION: {
        Message.writeString(buf, secType)
        (obj as ISection).write(buf)
      }
      case FILE: {
        if (obj instanceof File) {
          val file = obj as File
          Message.writeString(buf, file.name)
        } else {
          val name = obj as String
          Message.writeString(buf, name)
        }
      }
    }
  }
  
  static def Data read(ByteBuf buf) {
    val typeIndex = buf.readShort as int
    val type = Type.values.get(typeIndex)
    
    switch type {
      case EMPTY: {} //do nothing
      
      case RAW: {
        val obj = Message.readBytes(buf)
        new Data(obj)
      }
      
      case INTEGER: {
        val obj = buf.readInt
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
      
      case FILE: {
        val fileName = Message.readString(buf)
        new Data(Type.FILE, fileName, null)
      }
    }
  }
}
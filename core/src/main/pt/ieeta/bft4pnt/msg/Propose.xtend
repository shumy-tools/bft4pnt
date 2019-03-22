package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf

class Propose  implements ISection {
  
  override write(ByteBuf buf) {
    throw new UnsupportedOperationException("TODO: auto-generated method stub")
  }
  
  static def Propose read(ByteBuf buf) {
    
  }
}
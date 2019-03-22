package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf

class Update implements ISection {
  
  override write(ByteBuf buf) {
    throw new UnsupportedOperationException("TODO: auto-generated method stub")
  }
  
  static def Update read(ByteBuf buf) {
    
  }
}
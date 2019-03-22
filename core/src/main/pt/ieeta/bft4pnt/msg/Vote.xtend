package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf

class Vote implements ISection {
  
  override write(ByteBuf buf) {
    throw new UnsupportedOperationException("TODO: auto-generated method stub")
  }
  
  static def Vote read(ByteBuf buf) {
    
  }
}
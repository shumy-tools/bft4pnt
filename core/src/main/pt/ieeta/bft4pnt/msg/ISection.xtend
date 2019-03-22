package pt.ieeta.bft4pnt.msg

import io.netty.buffer.ByteBuf

interface ISection {
  def void write(ByteBuf buf)
}
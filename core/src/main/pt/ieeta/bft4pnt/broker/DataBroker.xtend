package pt.ieeta.bft4pnt.broker

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import java.net.InetSocketAddress
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.slf4j.LoggerFactory

@FinalFieldsConstructor
class IncommingPacketHandler extends SimpleChannelInboundHandler<DatagramPacket> {
  public val (InetSocketAddress, ByteBuf)=>void handler
  
  override protected channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
    handler.apply(packet.sender, packet.content)
  }
}

class BrokerInitializer extends ChannelInitializer<NioDatagramChannel> {
  public val IncommingPacketHandler packetHandler
  
  new((InetSocketAddress, ByteBuf)=>void handler) {
    this.packetHandler = new IncommingPacketHandler(handler)
  }
  
  override protected initChannel(NioDatagramChannel ch) throws Exception {
    ch.pipeline.addLast(this.packetHandler)
  }
}

class DataBroker {
  static val logger = LoggerFactory.getLogger(DataBroker)
  
  val InetSocketAddress address
  var Channel channel = null
  
  new(InetSocketAddress address) {
    this.address = address
  }
  
  def void start((InetSocketAddress, ByteBuf)=>void handler) {
    new Thread[
      Thread.currentThread.name = "MessageBroker-Thread"
      
      val group = new NioEventLoopGroup
      try {
        val b = new Bootstrap => [
          group(group)
          channel(NioDatagramChannel)
          handler(new BrokerInitializer(handler))
        ]
        
        channel = b.bind(address).sync.channel
        logger.info('BFT-PNT broker available at {}', address.port)
        
        channel.closeFuture.await
        logger.info('BFT-PNT broker stopped')
      } catch(Throwable ex) {
        ex.printStackTrace
      } finally {
        group.shutdownGracefully
      }
    ].start
  }
  
  def void send(InetSocketAddress target, ByteBuf data) {
    if (channel === null) return
    
    val packet = new DatagramPacket(data, target)
    channel.writeAndFlush(packet)
  }
}
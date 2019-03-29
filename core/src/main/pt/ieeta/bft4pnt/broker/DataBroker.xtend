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
  static val logger = LoggerFactory.getLogger(DataBroker.simpleName)
  
  val InetSocketAddress address
  var Channel channel = null
  
  new(InetSocketAddress address) {
    this.address = address
  }
  
  def isReady() { channel !== null }
  
  def void start((InetSocketAddress)=>void whenReady, (InetSocketAddress, ByteBuf)=>void handler) {
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
        logger.debug('BFT-PNT broker available at {}', address.port)
        whenReady.apply(address)
        
        channel.closeFuture.await
        logger.debug('BFT-PNT broker stopped')
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
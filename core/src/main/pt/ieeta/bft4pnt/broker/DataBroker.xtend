package pt.ieeta.bft4pnt.broker

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.FixedRecvByteBufAllocator
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.slf4j.LoggerFactory
import io.netty.buffer.PooledByteBufAllocator

@FinalFieldsConstructor
class IncommingPacketHandler extends SimpleChannelInboundHandler<DatagramPacket> {
  val (InetSocketAddress, ByteBuf)=>void handler
  
  override protected channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
    handler.apply(packet.sender, packet.content)
  }
}

class BrokerInitializer extends ChannelInitializer<NioDatagramChannel> {
  val IncommingPacketHandler packetHandler
  
  new((InetSocketAddress, ByteBuf)=>void handler) {
    this.packetHandler = new IncommingPacketHandler(handler)
  }
  
  override protected initChannel(NioDatagramChannel ch) throws Exception {
    ch.pipeline.addLast(this.packetHandler)
  }
}

@FinalFieldsConstructor
class DataBroker {
  static val logger = LoggerFactory.getLogger(DataBroker.simpleName)
  
  val InetSocketAddress address
  val channel = new AtomicReference<Channel>
  
  def isReady() { channel.get !== null }
  
  def void start((InetSocketAddress, ByteBuf)=>void handler) {
    new Thread[
      Thread.currentThread.name = "MessageBroker-Thread"
      
      val group = new NioEventLoopGroup(10)
      try {
        val b = new Bootstrap => [
          group(group)
          channel(NioDatagramChannel)
          handler(new BrokerInitializer(handler))
          
          //TODO: improve buffer received to a dynamic allocator?
          option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(16384))
          option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
          option(ChannelOption.SO_REUSEADDR, true)
        ]
        
        channel.set = b.bind(address).sync.channel
        logger.debug('BFT-PNT broker available at {}', address.port)
        
        channel.get.closeFuture.await
        logger.debug('BFT-PNT broker stopped')
      } catch(Throwable ex) {
        ex.printStackTrace
      } finally {
        group.shutdownGracefully.await
        Thread.currentThread.interrupt
        channel.set = null
      }
    ].start
  }
  
  def void stop() {
    if (channel.get !== null)
      channel.get.close.await
  }
  
  def void send(InetSocketAddress target, ByteBuf data) {
    val packet = new DatagramPacket(data, target)
    channel.get.writeAndFlush(packet)
  }
}
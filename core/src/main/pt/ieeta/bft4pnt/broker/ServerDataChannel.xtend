package pt.ieeta.bft4pnt.broker

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.msg.Get
import pt.ieeta.bft4pnt.msg.GetRetrieve
import pt.ieeta.bft4pnt.msg.HasSlices
import pt.ieeta.bft4pnt.msg.Message
import pt.ieeta.bft4pnt.msg.Slices
import pt.ieeta.bft4pnt.spi.PntDatabase

@FinalFieldsConstructor
class StorageHandler extends SimpleChannelInboundHandler<ByteBuf> {
  val ServerDataChannel srv
  
  override protected channelRead0(ChannelHandlerContext ctx, ByteBuf data) throws Exception {
    val result = Message.read(data)
    if (result.hasError) {
      ServerDataChannel.logger.error("CHANNEL-ERROR: {}", result.error.msg)
      return;
    }
    
    val msg = result.msg
    if (!srv.authorizer.apply(msg)) {
      ServerDataChannel.logger.error("Non authorized: {}", msg)
      return;
    }
    
    // retrieve data if it'a a GET request
    if (msg.type === Message.Type.GET) {
      val get = msg.body as Get
      ServerDataChannel.logger.info("SRV-RETRIEVE: (record={}, index={})", msg.record.fingerprint, get.index)
      
      if (get.slice == -1) {
        ctx.retrieveAll(msg.record.fingerprint, get.index)
        return
      }
      
      val cs = srv.db.store.getOrCreate(msg.record.udi)
      val slices = synchronized(cs) {
        // record must exist
        val record = cs.getRecord(msg.record.fingerprint)
        if (record === null) {
          ServerDataChannel.logger.error("Non existent record (get, rec={})", msg.record.fingerprint)
          return;
        }
        
        val index = if (get.index != -1) get.index else record.lastIndex
        val commit = record.getCommit(index)
        if (commit === null) {
          ServerDataChannel.logger.error("Non existent record (get, rec={}, idx={})", msg.record.fingerprint, get.index)
          return;
        }
        
        (commit.body as HasSlices).slices
      }
      
      ctx.retrieveSlice(slices, msg.record.fingerprint, get.index, get.slice)
    }
  }
  
  private def void retrieveAll(ChannelHandlerContext ctx, String record, int index) {
    val raf = srv.db.files.getFile(record, index)
    if (raf === null)
      return;
      
    val getRet = new GetRetrieve(raf.length, record, index, null)
    
    var buf = PooledByteBufAllocator.DEFAULT.buffer(ServerDataChannel.BUFFER_SIZE)
    getRet.write(buf)
    
    ctx.write(buf)
    
    if (raf.length === 0) {
      raf.close
      return
    }
    
    ctx.writeAndFlush(new ThrottlingFileRegion(raf.channel, 0, raf.length))
  }
  
  private def void retrieveSlice(ChannelHandlerContext ctx, Slices slices, String record, int index, int slice) {
    val raf = srv.db.files.getFile(record, index)
    if (raf === null)
      return;
    
    // last slice may be of different size
    val sSize = if (slice !== slices.slices.length - 1) {
      slices.size
    } else {
      val rem = raf.length % slices.size
      if (rem === 0) slices.size else rem
    }
    
    val getRet = new GetRetrieve(sSize, record, index, slices.slices.get(slice))
    
    var buf = PooledByteBufAllocator.DEFAULT.buffer(ServerDataChannel.BUFFER_SIZE)
    getRet.write(buf)
    
    ctx.write(buf)
    
    if (sSize === 0) {
      raf.close
      return
    }
    
    ctx.writeAndFlush(new ThrottlingFileRegion(raf.channel, slice * slices.size, sSize))
  }
}

@FinalFieldsConstructor
class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
  val ServerDataChannel srv
    
  override protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline.addLast(new StorageHandler(srv))
  }
}

@FinalFieldsConstructor
class ServerDataChannel {
  package static val BUFFER_SIZE = 4096
  package static val logger = LoggerFactory.getLogger(ServerDataChannel.simpleName)
  
  package val InetSocketAddress address
  package val PntDatabase db
  
  package var (Message)=>boolean authorizer
  
  package val channel = new AtomicReference<Channel>
  
  def isReady() { channel.get !== null }
  
  def void start((Message)=>boolean authorizer) {
    this.authorizer = authorizer
    
    new Thread[
      Thread.currentThread.name = "DataChannel-Thread"
      
      val bossGroup = new NioEventLoopGroup(1) as EventLoopGroup
      val workerGroup = new NioEventLoopGroup(8) as EventLoopGroup
      try {
        val b = new ServerBootstrap => [
          group(bossGroup, workerGroup)
          channel(NioServerSocketChannel)
          childHandler(new ServerChannelInitializer(this))
          
          option(ChannelOption.SO_KEEPALIVE, true)
          option(ChannelOption.TCP_NODELAY, true)
          option(ChannelOption.SO_RCVBUF, ClientDataChannel.BUF_SIZE)
          option(ChannelOption.SO_SNDBUF, ClientDataChannel.BUF_SIZE)
        ]
        
        channel.set = b.bind(address).sync.channel
        logger.debug('DATA-CHANNEL available at {}', address.port)
        
        channel.get.closeFuture.await
        logger.debug('DATA-CHANNEL stopped')
      } catch(Throwable ex) {
        ex.printStackTrace
      } finally {
        bossGroup.shutdownGracefully
        workerGroup.shutdownGracefully
        Thread.currentThread.interrupt
        channel.set = null
      }
    ].start
  }
}
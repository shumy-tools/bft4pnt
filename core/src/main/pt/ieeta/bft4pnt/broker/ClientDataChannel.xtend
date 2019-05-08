package pt.ieeta.bft4pnt.broker

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import java.io.File
import java.io.FileOutputStream
import java.net.InetSocketAddress
import java.security.KeyPair
import java.security.Signature
import java.util.concurrent.ConcurrentHashMap
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.slf4j.LoggerFactory
import pt.ieeta.bft4pnt.crypto.CryptoHelper
import pt.ieeta.bft4pnt.msg.GetRetrieve
import pt.ieeta.bft4pnt.msg.Message

@FinalFieldsConstructor
class RetrieveHandler extends SimpleChannelInboundHandler<ByteBuf> {
  val ClientDataChannel client
  
  var FileOutputStream os = null
  var GetRetrieve getRet = null
  var done = 0L
  
  override protected channelRead0(ChannelHandlerContext ctx, ByteBuf data) throws Exception {
    if (getRet === null) {
      getRet = GetRetrieve.read(data)
      
      // force directory creation
      new File(client.lStore).mkdirs
      
      val fileName  = getRet.record.replace("/", "_").replace("+", "-")
      val path = if (getRet.slice === null) {
        client.lStore + "/" + fileName + "-" + getRet.index
      } else {
        val sliceName = getRet.slice.replace("/", "_").replace("+", "-")
        client.lStore + "/" + fileName + "-" + getRet.index + "-" + sliceName
      }
      
      val file = new File(path)
      file.createNewFile
      
      os = new FileOutputStream(file)
    }
    
    val size = data.readableBytes
    if (size > 0)
      data.writeFile
    
    //println('''DONE(«size») «done» of «getRet.size»''')
    if (done === getRet.size) {
      os.flush
      os.close
      
      client.onSliceReady.apply(getRet.record, getRet.index, getRet.slice)
      ctx.channel.disconnect
    }
  }
  
  private def void writeFile(ByteBuf buf) {
    val length = buf.readableBytes
    val data = newByteArrayOfSize(length)
    buf.getBytes(buf.readerIndex, data)
    
    os.write(data)
    done += length
  }
}

@FinalFieldsConstructor
class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {
  val ClientDataChannel client
    
  override protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline.addLast(new RetrieveHandler(client))
  }
}

class ClientDataChannel {
  public static val BUF_SIZE = 8 * 1024 * 1024
  static val logger = LoggerFactory.getLogger(ClientDataChannel.simpleName)
  
  val KeyPair keys
  package val String lStore
  val Signature signer
  
  var NioEventLoopGroup group
  var Bootstrap boot
  
  package val channels = new ConcurrentHashMap<String, Channel> // <record, channel>
  package var (String, Integer, String)=>void onSliceReady // record, index, slice
  
  new(KeyPair keys, String lStore) {
    this.keys = keys
    this.lStore = lStore
    
    this.signer = Signature.getInstance(CryptoHelper.SIG_ALG, CryptoHelper.PROVIDER) => [
      initSign(keys.private)
    ]
  }
  
  def void start((String, Integer, String)=>void onSliceReady) {
    this.onSliceReady = onSliceReady
    
    group = new NioEventLoopGroup(8)
    try{
      boot = new Bootstrap => [
        group(group)
        channel(NioSocketChannel)
        handler(new ClientChannelInitializer(this))
        
        option(ChannelOption.TCP_NODELAY, true)
        option(ChannelOption.SO_RCVBUF, BUF_SIZE)
        option(ChannelOption.SO_SNDBUF, BUF_SIZE)
      ]
    } catch(Throwable ex) {
      ex.printStackTrace
    }
  }
  
  def close() {
    group?.shutdownGracefully.sync
  }
  
  def void request(InetSocketAddress adr, Message msg) {
    val channel = boot.connect(adr.hostString, adr.port).sync.channel
    channels.put(msg.record.fingerprint, channel)
    
    val data = msg.write(keys.public, signer)
    channel.writeAndFlush(data)
    logger.info("REQ-SENT: {} to {}", msg, '''«adr.hostString»:«adr.port»''')
    
    channel.closeFuture.await
    logger.info("REQ-COMPLETED: {} to {}", msg, '''«adr.hostString»:«adr.port»''')
  }
}
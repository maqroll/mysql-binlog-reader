package maqroll.github.com;

import com.github.mheath.netty.codec.mysql.CapabilityFlags;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.MysqlClientPacketEncoder;
import com.github.mheath.netty.codec.mysql.MysqlServerConnectionPacketDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.EnumSet;

public class BinlogConnection {
  private final NioEventLoopGroup eventLoopGroup;
  private final Bootstrap bootstrap;
  protected static final EnumSet<CapabilityFlags> CLIENT_CAPABILITIES =
      CapabilityFlags.getImplicitCapabilities();

  static {
    CLIENT_CAPABILITIES.addAll(
        EnumSet.of(
            CapabilityFlags.CLIENT_PLUGIN_AUTH,
            CapabilityFlags.CLIENT_SECURE_CONNECTION,
            CapabilityFlags.CLIENT_CONNECT_WITH_DB));
  }

  public BinlogConnection() {
    eventLoopGroup = new NioEventLoopGroup();
    bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            CapabilityFlags.setCapabilitiesAttr(ch, CLIENT_CAPABILITIES);
            ch.pipeline().addLast(new MysqlServerConnectionPacketDecoder());
            ch.pipeline().addLast(new MysqlClientPacketEncoder());
          }
        });

    ChannelFuture connectFuture = bootstrap.connect("localhost", 3306);

    try {
      connectFuture.addListener(
          (ChannelFutureListener)
              future -> {
                if (future.isSuccess()) {
                  future
                      .channel()
                      .pipeline()
                      .addLast(
                          new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg)
                                throws Exception {
                              if (msg instanceof Handshake) {
                                CapabilityFlags.getCapabilitiesAttr(ctx.channel())
                                    .retainAll(((Handshake) msg).getCapabilities());
                              }
                              // serverPackets.add((MysqlServerPacket) msg);
                            }
                          });
                }
              });
      connectFuture = connectFuture.sync();
      if (!connectFuture.isSuccess()) {
        throw new RuntimeException(connectFuture.cause());
      }
      Channel channel = connectFuture.channel();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}

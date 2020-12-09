package maqroll.github.com;

import io.netty.bootstrap.Bootstrap;
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
  }
}

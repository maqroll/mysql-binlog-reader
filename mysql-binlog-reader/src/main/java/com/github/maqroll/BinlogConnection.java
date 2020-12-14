package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.CapabilityFlags;
import com.github.mheath.netty.codec.mysql.MysqlClientPacketEncoder;
import com.github.mheath.netty.codec.mysql.MysqlServerConnectionPacketDecoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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

  public BinlogConnection(int port) {
    final ReplicationInboundHandler replicationInboundHandler = new ReplicationInboundHandler();
    final ServerInfo serverInfo = new ServerInfo(null, ChecksumType.CRC32); // TODO

    eventLoopGroup = new NioEventLoopGroup();
    eventLoopGroup.setIoRatio(1); //lag between deserialization and notification!!!
    bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            CapabilityFlags.setCapabilitiesAttr(ch, CLIENT_CAPABILITIES);
            ServerInfo.setServerInfoAttr(ch, serverInfo);

            ch.pipeline().addLast("serverDecoder", new MysqlServerConnectionPacketDecoder());
            ch.pipeline().addLast("clientEncoder", new MysqlClientPacketEncoder());
            ch.pipeline().addLast("binlogEncoder", new BinlogDumpEncoder());
            // TODO add the rest of handlers
            ch.pipeline().addLast("adapter", replicationInboundHandler);
          }
        });

    ChannelFuture connectFuture = bootstrap.connect("localhost", port);

    connectFuture = connectFuture.awaitUninterruptibly();
    if (!connectFuture.isSuccess()) {
      throw new RuntimeException(connectFuture.cause());
    }
    Channel channel = connectFuture.channel();
    ChannelFuture closeFuture = channel.closeFuture();

    closeFuture.awaitUninterruptibly();
  }
}

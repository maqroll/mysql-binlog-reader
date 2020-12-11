package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Constants;
import com.github.mheath.netty.codec.mysql.EofResponse;
import com.github.mheath.netty.codec.mysql.ErrorResponse;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.HandshakeResponse;
import com.github.mheath.netty.codec.mysql.MysqlNativePasswordUtil;
import com.github.mheath.netty.codec.mysql.MysqlPacket;
import com.github.mheath.netty.codec.mysql.MysqlServerPacketVisitor;
import com.github.mheath.netty.codec.mysql.OkResponse;
import com.github.mheath.netty.codec.mysql.QueryCommand;
import com.github.mheath.netty.codec.mysql.Visitable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.maqroll.BinlogConnection.CLIENT_CAPABILITIES;

public class Adapter extends ChannelInboundHandlerAdapter implements MysqlServerPacketVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(Adapter.class);
  private boolean updated = false;

  public Adapter() {}

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Visitable packet = (Visitable)msg;
    packet.accept(this, ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOGGER.info("Closing connection");
    ctx.close();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOGGER.error("Closing connection after uncaught exception", cause);
    ctx.close();
  }

  @Override
  public void visit(Handshake handshake, ChannelHandlerContext ctx) {
    LOGGER.info("Sending handshake response");
    HandshakeResponse response = HandshakeResponse
        .create()
        .addCapabilities(CLIENT_CAPABILITIES)
        .username("root")
        .addAuthData(MysqlNativePasswordUtil.hashPassword("root", handshake.getAuthPluginData()))
        .authPluginName(Constants.MYSQL_NATIVE_PASSWORD)
        .build();
    ctx.writeAndFlush(response);
  }

  @Override
  public void visit(OkResponse ok, ChannelHandlerContext ctx) {
    LOGGER.info("ok");
    // TODO is it necessary to check binlog value???
    if (!updated) {
      QueryCommand query = new QueryCommand(0, "set @master_binlog_checksum= @@global.binlog_checksum");
      ctx.writeAndFlush(query);
      updated = true;
    } else {
      LOGGER.info("Requesting binlog stream");
      BinlogDumpCommand binlogDumpCommand = BinlogDumpCommand.builder().fileName("").addFlags(BinlogDumpFlag.BINLOG_DUMP_NON_BLOCK).build();
      ctx.writeAndFlush(binlogDumpCommand);
      ctx.channel().pipeline().remove("serverDecoder");
      ctx.channel().pipeline().addFirst("replicationStreamDecoder", new ReplicationStreamDecoder());
    }
  }

  @Override
  public void visit(EofResponse eof, ChannelHandlerContext ctx) {
    LOGGER.info("eof");
  }

  @Override
  public void visit(ErrorResponse error, ChannelHandlerContext ctx) {
    LOGGER.info("error {}", error.getMessage());
  }
}

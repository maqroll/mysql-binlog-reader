package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.EofResponse;
import com.github.mheath.netty.codec.mysql.ErrorResponse;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.MysqlServerPacketVisitor;
import com.github.mheath.netty.codec.mysql.OkResponse;
import com.github.mheath.netty.codec.mysql.ReplicationEvent;
import com.github.mheath.netty.codec.mysql.Visitable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationInboundHandler extends ChannelInboundHandlerAdapter
    implements MysqlServerPacketVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationInboundHandler.class);
  private ReplicationState replicationState = ReplicationState.INIT;

  public ReplicationInboundHandler() {}

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Visitable packet = (Visitable) msg;
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
    replicationState = replicationState.handshake(handshake, ctx);
  }

  @Override
  public void visit(OkResponse ok, ChannelHandlerContext ctx) {
    replicationState = replicationState.ok(ok, ctx);
  }

  @Override
  public void visit(EofResponse eof, ChannelHandlerContext ctx) {
    replicationState = replicationState.eof(eof, ctx);
  }

  @Override
  public void visit(ErrorResponse error, ChannelHandlerContext ctx) {
    replicationState = replicationState.error(error, ctx);
  }

  @Override
  public void visit(ReplicationEvent repEvent, ChannelHandlerContext ctx) {
    LOGGER.info("Received replication event {}", repEvent);
  }
}

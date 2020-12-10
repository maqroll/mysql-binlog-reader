package com.github.maqroll;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Adapter extends ChannelInboundHandlerAdapter {
  private static final Logger LOGGER = LoggerFactory.getLogger(Adapter.class);

  public Adapter() {}

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    LOGGER.info(msg.toString());
    // TODO ignored so far
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
}

package com.github.maqroll;

import static com.github.maqroll.BinlogConnection.CLIENT_CAPABILITIES;

import com.github.mheath.netty.codec.mysql.Constants;
import com.github.mheath.netty.codec.mysql.EofResponse;
import com.github.mheath.netty.codec.mysql.ErrorResponse;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.HandshakeResponse;
import com.github.mheath.netty.codec.mysql.MysqlNativePasswordUtil;
import com.github.mheath.netty.codec.mysql.OkResponse;
import com.github.mheath.netty.codec.mysql.QueryCommand;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ReplicationState {
  INIT {
    @Override
    ReplicationState handshake(Handshake handshake, ChannelHandlerContext ctx) {
      LOGGER.info("Sending handshake response");
      // TODO user and password should be get from channel
      HandshakeResponse response =
          HandshakeResponse.create()
              .addCapabilities(CLIENT_CAPABILITIES)
              .username("root")
              .addAuthData(
                  MysqlNativePasswordUtil.hashPassword("root", handshake.getAuthPluginData()))
              .authPluginName(Constants.MYSQL_NATIVE_PASSWORD)
              .build();
      ctx.writeAndFlush(response);

      return WAITING_HANDSHAKE_RESPONSE;
    }
  },
  WAITING_HANDSHAKE_RESPONSE {
    @Override
    ReplicationState ok(OkResponse ok, ChannelHandlerContext ctx) {
      QueryCommand query =
          new QueryCommand(0, "set @master_binlog_checksum= @@global.binlog_checksum");
      ctx.writeAndFlush(query);
      return WAITING_CHECKSUM_CONFIRMATION;
    }
  },
  WAITING_CHECKSUM_CONFIRMATION {
    @Override
    ReplicationState ok(OkResponse ok, ChannelHandlerContext ctx) {
      ctx.channel().pipeline().remove("serverDecoder");
      ctx.channel()
          .pipeline()
          .addFirst("replicationStreamDecoder", new ReplicationStreamDecoder());

      BinlogDumpCommand binlogDumpCommand =
          BinlogDumpCommand.builder()
              .fileName("")
              //.addFlags(BinlogDumpFlag.BINLOG_DUMP_NON_BLOCK)
              .build();
      ctx.writeAndFlush(binlogDumpCommand);

      return REQUESTED_BINLOG_STREAM;
    }
  },
  REQUESTED_BINLOG_STREAM {
    // TODO
  };

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationState.class);

  ReplicationState handshake(Handshake handshake, ChannelHandlerContext ctx) {
    throw new IllegalStateException("Unexpected handshake message in state " + this);
  }

  ReplicationState ok(OkResponse ok, ChannelHandlerContext ctx) {
    throw new IllegalStateException("Unexpected ok message in state " + this);
  }

  ReplicationState eof(EofResponse eof, ChannelHandlerContext ctx) {
    throw new IllegalStateException("Unexpected eof message in state " + this);
  }

  ReplicationState error(ErrorResponse error, ChannelHandlerContext ctx) {
    throw new IllegalStateException("Unexpected error message in state " + this);
  }
}

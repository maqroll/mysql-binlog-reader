package com.github.maqroll;

import static com.github.maqroll.BinlogConnection.CLIENT_CAPABILITIES;
import static com.github.mheath.netty.codec.mysql.CapabilityFlags.CLIENT_DEPRECATE_EOF;

import com.github.mheath.netty.codec.mysql.ColumnCount;
import com.github.mheath.netty.codec.mysql.ColumnDefinition;
import com.github.mheath.netty.codec.mysql.Constants;
import com.github.mheath.netty.codec.mysql.EofResponse;
import com.github.mheath.netty.codec.mysql.ErrorResponse;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.HandshakeResponse;
import com.github.mheath.netty.codec.mysql.MysqlNativePasswordUtil;
import com.github.mheath.netty.codec.mysql.MysqlServerResultSetPacketDecoder;
import com.github.mheath.netty.codec.mysql.OkResponse;
import com.github.mheath.netty.codec.mysql.QueryCommand;
import com.github.mheath.netty.codec.mysql.ResultsetRow;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ReplicationState {
  INIT {
    @Override
    ReplicationState handshake(Handshake handshake, ChannelHandlerContext ctx) {
      LOGGER.info("Sending handshake response");

      ServerInfo serverInfo = ServerInfo.getCurrent(ctx.channel());

      HandshakeResponse response =
          HandshakeResponse.create()
              .addCapabilities(CLIENT_CAPABILITIES)
              .addCapabilities(CLIENT_DEPRECATE_EOF)
              .username(serverInfo.getEndpoint().getUser())
              .addAuthData(
                  MysqlNativePasswordUtil.hashPassword(
                      serverInfo.getEndpoint().getPassword(), handshake.getAuthPluginData()))
              .authPluginName(Constants.MYSQL_NATIVE_PASSWORD)
              .build();
      ctx.writeAndFlush(response);

      return WAITING_HANDSHAKE_RESPONSE;
    }
  },
  WAITING_HANDSHAKE_RESPONSE {
    @Override
    ReplicationState ok(OkResponse ok, ChannelHandlerContext ctx) {
      ctx.pipeline()
          .replace("serverDecoder", "serverDecoder", new MysqlServerResultSetPacketDecoder());
      QueryCommand query = new QueryCommand(0, "show global variables like 'binlog_checksum'");
      ctx.writeAndFlush(query);
      return WAITING_CHECKSUM_RESPONSE;
    }
  },
  WAITING_CHECKSUM_RESPONSE {
    @Override
    ReplicationState columnCount(ColumnCount columnCount, ChannelHandlerContext ctx) {
      return this;
    }

    @Override
    ReplicationState columnDefinition(
        ColumnDefinition columnDefinition, ChannelHandlerContext ctx) {
      return this;
    }

    @Override
    ReplicationState resultSetRow(ResultsetRow resultsetRow, ChannelHandlerContext ctx) {
      ResultSet.getCurrent(ctx.channel()).addRow(resultsetRow);
      return this;
    }

    @Override
    ReplicationState ok(OkResponse ok, ChannelHandlerContext ctx) {
      ResultSet rs = ResultSet.getCurrent(ctx.channel());

      List<ResultsetRow> rows = rs.rows();
      if (rows.size() == 1) {
        String checksum = rows.get(0).getValues().get(1);
        ChecksumType checksumType = ChecksumType.valueOf(checksum);

        if (checksumType == null) {
          throw new IllegalArgumentException("binlog_checksum value not supported: " + checksum);
        }

        ServerInfo.getCurrent(ctx.channel()).setChecksumType(checksumType);
      } else {
        ServerInfo.getCurrent(ctx.channel()).setChecksumType(ChecksumType.NONE);
      }

      ctx.pipeline()
          .replace("serverDecoder", "serverDecoder", new MysqlServerResultSetPacketDecoder());
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
      ctx.channel().pipeline().addFirst("replicationStreamDecoder", new ReplicationStreamDecoder());

      BinlogDumpCommand binlogDumpCommand =
          BinlogDumpCommand.builder()
              .fileName("")
              // .addFlags(BinlogDumpFlag.BINLOG_DUMP_NON_BLOCK)
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

  ReplicationState columnCount(ColumnCount columnCount, ChannelHandlerContext ctx) {
    throw new IllegalStateException("Unexpected column count message in state " + this);
  }

  ReplicationState columnDefinition(ColumnDefinition columnDefinition, ChannelHandlerContext ctx) {
    throw new IllegalStateException("Unexpected column definition message in state " + this);
  }

  ReplicationState resultSetRow(ResultsetRow resultsetRow, ChannelHandlerContext ctx) {
    throw new IllegalStateException("Unexpected row message in state " + this);
  }
}

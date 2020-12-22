package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ColumnCount;
import com.github.mheath.netty.codec.mysql.ColumnDefinition;
import com.github.mheath.netty.codec.mysql.ColumnType;
import com.github.mheath.netty.codec.mysql.EofResponse;
import com.github.mheath.netty.codec.mysql.ErrorResponse;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.MysqlServerPacketVisitor;
import com.github.mheath.netty.codec.mysql.OkResponse;
import com.github.mheath.netty.codec.mysql.ReplicationEvent;
import com.github.mheath.netty.codec.mysql.ResultsetRow;
import com.github.mheath.netty.codec.mysql.Row;
import com.github.mheath.netty.codec.mysql.RowVisitor;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitable;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitor;
import com.github.mheath.netty.codec.mysql.Visitable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationInboundHandler extends ChannelInboundHandlerAdapter
    implements MysqlServerPacketVisitor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationInboundHandler.class);
  private ReplicationState replicationState = ReplicationState.INIT;
  private long c = 0L;

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
  public void visit(ColumnCount columnCount, ChannelHandlerContext ctx) {
    replicationState = replicationState.columnCount(columnCount, ctx);
  }

  @Override
  public void visit(ColumnDefinition columnDefinition, ChannelHandlerContext ctx) {
    replicationState = replicationState.columnDefinition(columnDefinition, ctx);
  }

  @Override
  public void visit(ResultsetRow row, ChannelHandlerContext ctx) {
    replicationState = replicationState.resultSetRow(row, ctx);
  }

  @Override
  public void visit(ReplicationEvent repEvent, ChannelHandlerContext ctx) {
    LOGGER.info("Received replication event {}", repEvent);

    final RowsChangedVisitable payload = (RowsChangedVisitable) repEvent.payload();

    payload.accept(
        new RowsChangedVisitor() {
          @Override
          public void added(String db, String table, Stream<Row> rows) {
            rows.forEach(
                row -> {
                  row.accept(
                      new RowVisitor() {
                        @Override
                        public void visit(int idx, ColumnType type) {
                          LOGGER.info("Added {}.{} {} {}", db, table, idx, type.toString());
                        }
                      });
                });
          }

          @Override
          public void removed(String db, String table, Stream<Row> rows) {
            rows.forEach(
                row -> {
                  row.accept(
                      new RowVisitor() {
                        @Override
                        public void visit(int idx, ColumnType type) {
                          LOGGER.info("Removed {}.{} {} {}", db, table, idx, type.toString());
                        }
                      });
                });
          }

          @Override
          public void updated(String db, String table, Stream<Row> rows) {
            rows.forEach(
                row -> {
                  row.accept(
                      new RowVisitor() {
                        @Override
                        public void visit(int idx, ColumnType type) {
                          LOGGER.info("Updated {}.{} {} {}", db, table, idx, type.toString());
                        }
                      });
                });
          }
        });
  }
}

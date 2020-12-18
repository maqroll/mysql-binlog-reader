package com.github.maqroll;

import com.github.maqroll.deserializers.ReplicationEventPayloadDeserializer;
import com.github.maqroll.deserializers.RotateEventDeserializer;
import com.github.maqroll.deserializers.TableMapEventDeserializer;
import com.github.maqroll.deserializers.WriteRowsEventDeserializer;
import com.github.mheath.netty.codec.mysql.AbstractPacketDecoder;
import com.github.mheath.netty.codec.mysql.CapabilityFlags;
import com.github.mheath.netty.codec.mysql.MysqlCharacterSet;
import com.github.mheath.netty.codec.mysql.MysqlServerPacketDecoder;
import com.github.mheath.netty.codec.mysql.ReplicationEvent;
import com.github.mheath.netty.codec.mysql.ReplicationEventHeader;
import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import com.github.mheath.netty.codec.mysql.ReplicationEventType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ReplicationStreamDecoder extends AbstractPacketDecoder
    implements MysqlServerPacketDecoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationStreamDecoder.class);

  private final AtomicBoolean init = new AtomicBoolean();
  private ParallelDeserializer parallelDeserializer;
  private final Map<ReplicationEventType, ReplicationEventPayloadDeserializer<?>> deserializers =
      new HashMap<>();

  public ReplicationStreamDecoder() {
    this(DEFAULT_MAX_PACKET_SIZE);
  }

  public ReplicationStreamDecoder(int maxPacketSize) {
    super(maxPacketSize);
    deserializers.put(ReplicationEventType.ROTATE_EVENT, new RotateEventDeserializer());
    deserializers.put(ReplicationEventType.TABLE_MAP_EVENT, new TableMapEventDeserializer());
    deserializers.put(ReplicationEventType.WRITE_ROWS_EVENTv1, new WriteRowsEventDeserializer());
  }

  private void init(ChannelHandlerContext ctx) {
    parallelDeserializer = new ParallelDeserializer(1, ctx);
    init.set(true);
  }

  private void injectDeserializedMessages(ChannelHandlerContext ctx) {
    final Future<ReplicationEvent> task = parallelDeserializer.getTask();

    if (task != null) {
      if (task.isDone()) {
        processResult(task, ctx);
      }
    }

    if (parallelDeserializer.pending()) {
      ctx.executor().schedule(() -> injectDeserializedMessages(ctx), 1, TimeUnit.MILLISECONDS);
    }
  }

  private void processResult(Future<ReplicationEvent> fEvent, ChannelHandlerContext ctx) {
    try {
      ReplicationEvent replicationEvent = fEvent.get();
      ctx.fireChannelRead(replicationEvent);
    } catch (InterruptedException e) {
      // TODO
      e.printStackTrace();
      ctx.fireExceptionCaught(e);
    } catch (ExecutionException e) {
      // TODO
      e.printStackTrace();
      ctx.fireExceptionCaught(e.getCause());
    }
  }

  @Override
  protected void decodePacket(
      ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out) {
    if (!init.get()) init(ctx);
    final Channel channel = ctx.channel();
    final Set<CapabilityFlags> capabilities = CapabilityFlags.getCapabilitiesAttr(channel);
    final ServerInfo serverInfo = ServerInfo.getServerInfoAttr(channel);
    final ChecksumType checksum = serverInfo.getChecksumType();
    final Charset serverCharset = MysqlCharacterSet.getServerCharsetAttr(channel).getCharset();

    // TODO ojo al splitting del paquete
    // TODO ojito a los interbloqueos entre la producción y el consumo
    final int status = packet.readByte() & 0xff;
    switch (status) {
      case RESPONSE_OK:
        // TODO
        ReplicationEventHeader header = decodeHeader(packet);
        if (ReplicationEventType.ROTATE_EVENT.equals(header.getEventType())
            || ReplicationEventType.TABLE_MAP_EVENT.equals(header.getEventType())
            || ReplicationEventType.WRITE_ROWS_EVENTv1.equals(header.getEventType())) {
          int length = packet.readableBytes() - checksum.getValue();
          // packet = packet.readRetainedSlice(length);
          // System.out.println(ByteBufUtil.prettyHexDump(packet));

          // System.out.println("." + packet.refCnt());
          // parallelDeserializer.addPacket(
          //    header, packet.retainedSlice(packet.readerIndex(),length),
          // serverInfo.getChecksumType(), channel);
          LOGGER.info("Received " + header.getEventType());
          /*        if (parallelDeserializer.pending()) {
            ctx.executor().schedule(() -> injectDeserializedMessages(ctx), 5, TimeUnit.MILLISECONDS);
          }*/
          final ReplicationEventPayloadDeserializer<?> deserializer =
              deserializers.get(header.getEventType());
          ReplicationEventPayload payload =
              deserializer.deserialize(packet.readSlice(length), ctx.channel());

          if (header.getEventType().equals(ReplicationEventType.TABLE_MAP_EVENT)) {
            ((TableMapEventPayload) payload).setCurrent(ctx.channel());
          }
          out.add(new ReplicationEventImpl(header, payload));
          packet.skipBytes(checksum.getValue());
        }
        break;
      case RESPONSE_EOF:
        // TODO
        System.out.println("Received EOF");
        break;
      case RESPONSE_ERROR:
        // TODO should read the rest of packet as an error packet
        System.out.println("Received ERROR");
        break;
      default:
        // TODO
        throw new UnsupportedOperationException("Unknown replication status");
    }
  }

  protected ReplicationEventHeader decodeHeader(ByteBuf packet) {

    final ReplicationEventHeader header =
        ReplicationEventHeader.builder()
            .timestamp(packet.readUnsignedIntLE())
            .eventType(ReplicationEventType.lookup(packet.readByte()))
            .serverId(packet.readUnsignedIntLE())
            .eventLength(packet.readUnsignedIntLE())
            .nextPosition(packet.readUnsignedIntLE())
            .flags(packet.readShortLE())
            .build();

    return header;
  }
}

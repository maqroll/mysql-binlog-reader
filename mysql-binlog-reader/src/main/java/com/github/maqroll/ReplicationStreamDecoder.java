package com.github.maqroll;

import static com.github.maqroll.Deserializers.get;

import com.github.maqroll.deserializers.ReplicationEventPayloadDeserializer;
import com.github.mheath.netty.codec.mysql.AbstractPacketDecoder;
import com.github.mheath.netty.codec.mysql.CapabilityFlags;
import com.github.mheath.netty.codec.mysql.MysqlCharacterSet;
import com.github.mheath.netty.codec.mysql.MysqlServerPacketDecoder;
import com.github.mheath.netty.codec.mysql.ReplicationEvent;
import com.github.mheath.netty.codec.mysql.ReplicationEventHeader;
import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import com.github.mheath.netty.codec.mysql.ReplicationEventType;
import com.github.mheath.netty.codec.mysql.RotateEventPayload;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.List;
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
  private final boolean parallel;
  private final EnumSet<ReplicationEventType> interestingTypes =
      EnumSet.of(
          ReplicationEventType.ROTATE_EVENT,
          ReplicationEventType.TABLE_MAP_EVENT,
          ReplicationEventType.WRITE_ROWS_EVENTv1,
          ReplicationEventType.UPDATE_ROWS_EVENTv1,
          ReplicationEventType.DELETE_ROWS_EVENTv1);

  private final EnumSet<ReplicationEventType> typesToNotifyUpwards =
      EnumSet.of(
          ReplicationEventType.WRITE_ROWS_EVENTv1,
          ReplicationEventType.UPDATE_ROWS_EVENTv1,
          ReplicationEventType.DELETE_ROWS_EVENTv1);

  private String filename;

  public ReplicationStreamDecoder() {
    this(DEFAULT_MAX_PACKET_SIZE, true);
  }

  public ReplicationStreamDecoder(int maxPacketSize, boolean parallel) {
    super(maxPacketSize);
    this.parallel = parallel;
  }

  public String getFilename() {
    return this.filename;
  }

  public void updateFilename(String filename) {
    this.filename = filename;
  }

  private void init(ChannelHandlerContext ctx) {
    if (parallel) {
      parallelDeserializer = new ParallelDeserializer(10, ctx, this, typesToNotifyUpwards);
    }
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
    final ConnectionInfo connectionInfo = ConnectionInfo.getCurrent(channel);
    final ChecksumType checksum = connectionInfo.getChecksumType();
    final Charset serverCharset = MysqlCharacterSet.getServerCharsetAttr(channel).getCharset();

    // TODO ojo al splitting del paquete
    // TODO ojito a los interbloqueos entre la producción y el consumo
    final int status = packet.readByte() & 0xff;
    switch (status) {
      case RESPONSE_OK:
        ReplicationEventHeader header = decodeHeader(packet);
        if (interestingTypes.contains(header.getEventType())) {
          int length = packet.readableBytes() - checksum.getValue();
          if (parallel) {
            parallelDeserializer.addPacket(
                header,
                packet.readRetainedSlice(length),
                connectionInfo.getChecksumType(),
                channel);
          } else {
            final ReplicationEventPayloadDeserializer<?> deserializer = get(header.getEventType());
            ReplicationEventPayload payload =
                deserializer.deserialize(packet.readSlice(length), ctx.channel());

            if (header.getEventType().equals(ReplicationEventType.TABLE_MAP_EVENT)) {
              ((TableMapEventPayload) payload).setCurrent(ctx.channel());
            }

            if (header.getEventType().equals(ReplicationEventType.ROTATE_EVENT)) {
              RotateEventPayload rotate = (RotateEventPayload) payload;
              this.filename = rotate.getFilename();
              out.add(
                  new ReplicationEventImpl(
                      header, payload, new ROPositionImpl(this.filename, rotate.getPos())));
            } else {
              out.add(
                  new ReplicationEventImpl(
                      header,
                      payload,
                      new ROPositionImpl(this.filename, header.getNextPosition())));
            }

            packet.skipBytes(checksum.getValue());
          }
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

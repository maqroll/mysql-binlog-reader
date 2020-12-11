package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.AbstractPacketDecoder;
import com.github.mheath.netty.codec.mysql.CapabilityFlags;
import com.github.mheath.netty.codec.mysql.CodecUtils;
import com.github.mheath.netty.codec.mysql.Constants;
import com.github.mheath.netty.codec.mysql.Handshake;
import com.github.mheath.netty.codec.mysql.MysqlCharacterSet;
import com.github.mheath.netty.codec.mysql.MysqlServerPacketDecoder;
import com.github.mheath.netty.codec.mysql.OkResponse;
import com.github.mheath.netty.codec.mysql.ServerStatusFlag;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.util.CharsetUtil;

import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ReplicationStreamDecoder extends AbstractPacketDecoder implements MysqlServerPacketDecoder {

  public ReplicationStreamDecoder() {
    this(DEFAULT_MAX_PACKET_SIZE);
  }

  public ReplicationStreamDecoder(int maxPacketSize) {
    super(maxPacketSize);
  }

  @Override
  protected void decodePacket(ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out) {
    final Channel channel = ctx.channel();
    final Set<CapabilityFlags> capabilities = CapabilityFlags.getCapabilitiesAttr(channel);
    final Charset serverCharset = MysqlCharacterSet.getServerCharsetAttr(channel).getCharset();

    // TODO ojo al splitting del paquete
    final int status = packet.readByte() & 0xff;
    switch (status) {
      case RESPONSE_OK:
        // TODO
        ReplicationEventHeader header = decodeHeader(packet);
        System.out.println("Received " + header.getEventType());
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

    final ReplicationEventHeader header = ReplicationEventHeader.builder()
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

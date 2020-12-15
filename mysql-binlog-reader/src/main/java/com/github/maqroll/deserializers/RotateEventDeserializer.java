package com.github.maqroll.deserializers;

import com.github.mheath.netty.codec.mysql.CodecUtils;
import com.github.mheath.netty.codec.mysql.RotateEventPayload;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.nio.charset.Charset;

public class RotateEventDeserializer
    implements ReplicationEventPayloadDeserializer<RotateEventPayload> {

  @Override
  public RotateEventPayload deserialize(ByteBuf buf, Channel ch) {
    long pos = buf.readLong();
    String filename = CodecUtils.readFixedLengthString(buf, buf.readableBytes(), Charset.defaultCharset()); // TODO charset

    return new RotateEventPayload(pos, filename);
  }
}

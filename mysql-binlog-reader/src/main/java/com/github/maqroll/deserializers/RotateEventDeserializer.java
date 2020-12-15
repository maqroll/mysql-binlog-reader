package com.github.maqroll.deserializers;

import com.github.mheath.netty.codec.mysql.CodecUtils;
import com.github.mheath.netty.codec.mysql.RotateEventPayload;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;

public class RotateEventDeserializer
    implements ReplicationEventPayloadDeserializer<RotateEventPayload> {

  @Override
  public RotateEventPayload deserialize(ByteBuf buf, Charset charset) {
    long pos = buf.readLong();
    String filename = CodecUtils.readFixedLengthString(buf, buf.readableBytes(), charset);

    return new RotateEventPayload(pos, filename);
  }
}

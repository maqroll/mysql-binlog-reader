package com.github.maqroll.deserializers;

import com.github.maqroll.TableMapEventPayload;
import com.github.maqroll.WriteRowsEventPayload;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.nio.charset.Charset;

public class WriteRowsEventDeserializer
    implements ReplicationEventPayloadDeserializer<WriteRowsEventPayload> {

  @Override
  public WriteRowsEventPayload deserialize(ByteBuf buf, Channel ch) {
    final TableMapEventPayload tableMapEventPayload = TableMapEventPayload.getCurrent(ch);

    final WriteRowsEventPayload.Builder builder = WriteRowsEventPayload.builder();

    // TODO

    return builder.build();
  }
}

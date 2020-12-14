package com.github.maqroll;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public class TableMapEventDeserializer
    implements ReplicationEventPayloadDeserializer<TableMapEventPayload> {

  @Override
  public TableMapEventPayload deserialize(ByteBuf buf, Charset charset) {
    final TableMapEventPayload.Builder builder = TableMapEventPayload.builder();

    // TODO

    return builder.build();
  }
}

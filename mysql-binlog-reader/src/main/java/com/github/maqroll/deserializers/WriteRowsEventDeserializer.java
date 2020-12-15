package com.github.maqroll.deserializers;

import com.github.maqroll.TableMapEventPayload;
import com.github.maqroll.WriteRowsEventPayload;
import com.github.mheath.netty.codec.mysql.CodecUtils;
import com.github.mheath.netty.codec.mysql.ColumnType;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class WriteRowsEventDeserializer
    implements ReplicationEventPayloadDeserializer<WriteRowsEventPayload> {

  @Override
  public WriteRowsEventPayload deserialize(ByteBuf buf, Charset charset) {
    final WriteRowsEventPayload.Builder builder = WriteRowsEventPayload.builder();

    // TODO

    return builder.build();
  }
}

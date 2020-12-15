package com.github.maqroll.deserializers;

import com.github.maqroll.TableMapEventPayload;
import com.github.maqroll.Utils;
import com.github.maqroll.WriteRowsEventPayload;
import com.github.mheath.netty.codec.mysql.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

public class WriteRowsEventDeserializer
    implements ReplicationEventPayloadDeserializer<WriteRowsEventPayload> {

  @Override
  public WriteRowsEventPayload deserialize(ByteBuf buf, Channel ch) {
    final TableMapEventPayload tableMapEventPayload = TableMapEventPayload.getCurrent(ch);

    final WriteRowsEventPayload.Builder builder = WriteRowsEventPayload.builder();

    buf.skipBytes(6); // TODO we could check that tableId is ok
    buf.skipBytes(2); // flags
    // TODO mayContainExtraInformation (V2)
    long columnCount = CodecUtils.readLengthEncodedInteger(buf);
    BitSet columnsSent = Utils.readBitSet(buf, (int) columnCount);

    builder.tableMap(tableMapEventPayload);
    builder.columnCount(columnCount);
    builder.columnsSent(columnsSent);
    builder.rows(deserializeRows(tableMapEventPayload, columnsSent, buf));
    return builder.build();
  }

  private List<Object[]> deserializeRows(TableMapEventPayload tableMap, BitSet includedColumns, ByteBuf buf) {
    List<Object[]> result = new LinkedList<Object[]>();
    /*while (buf.isReadable()) {
      result.add(deserializeRow(tableId, includedColumns, inputStream));
    }*/
    return result;
  }

}

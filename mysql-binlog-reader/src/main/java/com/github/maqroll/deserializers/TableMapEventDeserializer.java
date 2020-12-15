package com.github.maqroll.deserializers;

import com.github.maqroll.TableMapEventPayload;
import com.github.maqroll.Utils;
import com.github.mheath.netty.codec.mysql.CodecUtils;
import com.github.mheath.netty.codec.mysql.ColumnType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class TableMapEventDeserializer
    implements ReplicationEventPayloadDeserializer<TableMapEventPayload> {

  @Override
  public TableMapEventPayload deserialize(ByteBuf buf, Channel ch) {
    final TableMapEventPayload.Builder builder = TableMapEventPayload.builder();

    builder.tableId(CodecUtils.readUnsignedLongLE(buf, 6));
    buf.skipBytes(3);
    builder.database(CodecUtils.readNullTerminatedString(buf, Charset.defaultCharset())); // TODO
    buf.skipBytes(1);
    builder.table(CodecUtils.readNullTerminatedString(buf, Charset.defaultCharset())); // TODO
    long numberOfColumns = (int) CodecUtils.readLengthEncodedInteger(buf);
    List<ColumnType> columnTypes = new ArrayList<>();
    for (int i = 0; i < numberOfColumns; i++) {
      columnTypes.add(ColumnType.lookup(buf.readUnsignedByte()));
    }
    builder.columnTypes(columnTypes);
    int metadataBlockSize = (int) CodecUtils.readLengthEncodedInteger(buf);
    buf.skipBytes(metadataBlockSize); // TODO at least decode column names
    builder.columnNullability(Utils.readBitSet(buf,(int) numberOfColumns));

    // in MySQL could be followed by optional metadata
    // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#Table_table_map_event_column_types
    return builder.build();
  }
}

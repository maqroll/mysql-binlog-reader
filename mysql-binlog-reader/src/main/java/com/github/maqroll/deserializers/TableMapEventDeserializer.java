package com.github.maqroll.deserializers;

import com.github.maqroll.TableMapEventPayload;
import com.github.mheath.netty.codec.mysql.CodecUtils;
import com.github.mheath.netty.codec.mysql.ColumnType;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class TableMapEventDeserializer
    implements ReplicationEventPayloadDeserializer<TableMapEventPayload> {

  @Override
  public TableMapEventPayload deserialize(ByteBuf buf, Charset charset) {
    final TableMapEventPayload.Builder builder = TableMapEventPayload.builder();

    builder.tableId(CodecUtils.readUnsignedLongLE(buf, 6));
    buf.skipBytes(3);
    builder.database(CodecUtils.readNullTerminatedString(buf, charset));
    buf.skipBytes(1);
    builder.table(CodecUtils.readNullTerminatedString(buf, charset));
    long numberOfColumns = (int) CodecUtils.readLengthEncodedInteger(buf);
    List<ColumnType> columnTypes = new ArrayList<>();
    for (int i = 0; i < numberOfColumns; i++) {
      columnTypes.add(ColumnType.lookup(buf.readUnsignedByte()));
    }
    builder.columnTypes(columnTypes);
    int metadataBlockSize = (int) CodecUtils.readLengthEncodedInteger(buf);
    buf.skipBytes(metadataBlockSize); // TODO probably not needed
    int nullSize = (int) ((numberOfColumns + 8) / 7);
    byte[] nullsBytes = new byte[nullSize];
    buf.readBytes(nullsBytes);
    builder.columnNullability(BitSet.valueOf(nullsBytes));

    // in MySQL could be followed by optional metadata
    // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#Table_table_map_event_column_types
    return builder.build();
  }
}

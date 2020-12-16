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

    builder.tableId(CodecUtils.readUnsignedLongLE6(buf));
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
    builder.columnMetadata(readMetadata(buf, columnTypes));
    builder.columnNullability(Utils.readBitSet(buf, (int) numberOfColumns));

    // TODO at least decode column names (in MySQL, not in MariaDB)
    // in MySQL could be followed by optional metadata
    // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#Table_table_map_event_column_types
    // In MariaDB there is no column names/signedness in metadata.
    return builder.build();
  }

  private List<Integer> readMetadata(ByteBuf buf, List<ColumnType> columnTypes) {
    List<Integer> metadata = new ArrayList<>();
    for (ColumnType colType : columnTypes) {
      switch (colType) {
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_GEOMETRY:
          metadata.add(new Integer(buf.readUnsignedByte()));
          break;
        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_NEWDECIMAL:
          metadata.add(new Integer(buf.readUnsignedShortLE()));
          break;
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_STRING:
          metadata.add(new Integer(buf.readUnsignedShort()));
          break;
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP2:
          metadata.add(new Integer(buf.readUnsignedByte()));
          break;
        default:
          metadata.add(new Integer(0));
      }
    }
    return metadata;
  }
}

package com.github.maqroll.deserializers;

import com.github.maqroll.TableMapEventPayload;
import com.github.maqroll.Utils;
import com.github.maqroll.WriteRowsEventPayload;
import com.github.mheath.netty.codec.mysql.CodecUtils;
import com.github.mheath.netty.codec.mysql.ColumnType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteRowsEventDeserializer
    implements ReplicationEventPayloadDeserializer<WriteRowsEventPayload> {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteRowsEventDeserializer.class);

  @Override
  public WriteRowsEventPayload deserialize(ByteBuf buf, Channel ch) {
    final TableMapEventPayload tableMapEventPayload = TableMapEventPayload.getCurrent(ch);

    final WriteRowsEventPayload.Builder builder = WriteRowsEventPayload.builder();

    // System.out.println(ByteBufUtil.prettyHexDump(buf));

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

  private List<Object[]> deserializeRows(
      TableMapEventPayload tableMap, BitSet includedColumns, ByteBuf buf) {
    List<Object[]> result = new LinkedList<Object[]>();
    while (buf.isReadable()) {
      result.add(deserializeRow(tableMap, includedColumns, buf));
    }
    return result;
  }

  // TODO move to a common hierarchy of row deserializers
  protected Object[] deserializeRow(
      TableMapEventPayload tableMap, BitSet includedColumns, ByteBuf buf) {
    List<ColumnType> types = tableMap.getColumnTypes();
    List<Integer> metadata = tableMap.getColumnMetadata();
    Object[] result = new Object[includedColumns.cardinality()];
    BitSet nullColumns = Utils.readBitSet(buf, result.length);

    for (int i = 0, numberOfSkippedColumns = 0; i < types.size(); i++) {
      if (!includedColumns.get(i)) {
        numberOfSkippedColumns++;
        continue;
      }
      int index = i - numberOfSkippedColumns;
      if (!nullColumns.get(index)) {
        // mysql-5.6.24 sql/log_event.cc log_event_print_value (line 1980)
        ColumnType type = types.get(i);
        int typeCode = type.getValue();
        int meta = metadata.get(i), length = 0;
        if (ColumnType.MYSQL_TYPE_STRING.equals(type)) {
          if (meta >= 256) {
            int meta0 = meta >> 8, meta1 = meta & 0xFF;
            if ((meta0 & 0x30) != 0x30) {
              typeCode = meta0 | 0x30;
              length = meta1 | (((meta0 & 0x30) ^ 0x30) << 4);
            } else {
              // mysql-5.6.24 sql/rpl_utility.h enum_field_types (line 278)
              if (meta0 == ColumnType.MYSQL_TYPE_ENUM.getValue()
                  || meta0 == ColumnType.MYSQL_TYPE_SET.getValue()) {
                typeCode = meta0;
              }
              length = meta1;
            }
          } else {
            length = meta;
          }
        }
        result[index] = deserializeCell(ColumnType.lookup(typeCode), meta, length, buf);
      }
    }
    return result;
  }

  private Object deserializeCell(ColumnType type, int meta, int length, ByteBuf buf) {
    switch (type) {
      case MYSQL_TYPE_BIT:
        return DeserializerUtils.deserializeBit(meta, buf);
      case MYSQL_TYPE_TINY:
        return DeserializerUtils.deserializeTiny(buf);
      case MYSQL_TYPE_SHORT:
        return DeserializerUtils.deserializeShort(buf);
      case MYSQL_TYPE_INT24:
        return DeserializerUtils.deserializeInt24(buf);
      case MYSQL_TYPE_LONG:
        return DeserializerUtils.deserializeLong(buf);
      case MYSQL_TYPE_LONGLONG:
        return DeserializerUtils.deserializeLongLong(buf);
      case MYSQL_TYPE_FLOAT:
        return DeserializerUtils.deserializeFloat(buf);
      case MYSQL_TYPE_DOUBLE:
        return DeserializerUtils.deserializeDouble(buf);
      case MYSQL_TYPE_NEWDECIMAL:
        return DeserializerUtils.deserializeNewDecimal(meta, buf);
      case MYSQL_TYPE_DATE:
        return DeserializerUtils.deserializeDate(buf);
      case MYSQL_TYPE_TIME:
        return DeserializerUtils.deserializeTime(buf);
      case MYSQL_TYPE_TIME2:
        return DeserializerUtils.deserializeTimeV2(meta, buf);
      case MYSQL_TYPE_TIMESTAMP:
        return DeserializerUtils.deserializeTimestamp(buf);
      case MYSQL_TYPE_TIMESTAMP2:
        return DeserializerUtils.deserializeTimestampV2(meta, buf);
      case MYSQL_TYPE_DATETIME:
        return DeserializerUtils.deserializeDatetime(buf);
      case MYSQL_TYPE_DATETIME2:
        return DeserializerUtils.deserializeDatetimeV2(meta, buf);
      case MYSQL_TYPE_YEAR:
        return DeserializerUtils.deserializeYear(buf);
      case MYSQL_TYPE_STRING: // CHAR or BINARY
        return DeserializerUtils.deserializeString(length, buf);
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING: // VARCHAR or VARBINARY
        return DeserializerUtils.deserializeVarString(meta, buf);
      case MYSQL_TYPE_BLOB:
        return DeserializerUtils.deserializeBlob(meta, buf);
      case MYSQL_TYPE_ENUM:
        return DeserializerUtils.deserializeEnum(length, buf);
      case MYSQL_TYPE_SET:
        return DeserializerUtils.deserializeSet(length, buf);
      case MYSQL_TYPE_GEOMETRY:
        return DeserializerUtils.deserializeGeometry(meta, buf);
      case MYSQL_TYPE_JSON:
        return DeserializerUtils.deserializeJson(meta, buf);
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }
  }
}

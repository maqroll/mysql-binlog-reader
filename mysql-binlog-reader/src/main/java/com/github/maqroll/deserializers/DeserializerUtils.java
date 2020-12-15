package com.github.maqroll.deserializers;

import com.github.maqroll.Utils;
import com.github.mheath.netty.codec.mysql.CodecUtils;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.Calendar;
import java.util.TimeZone;

// Adapted from
// com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer
public class DeserializerUtils {
  private static final int DIG_PER_DEC = 9;
  private static final int[] DIG_TO_BYTES = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

  private DeserializerUtils() {}

  // BIT
  /*
   A 1 byte unsigned int representing the length in bits of the bitfield (0 to 64),
   followed by a 1 byte unsigned int representing the number of bytes occupied by the bitfield.
   The number of bytes is either int((length + 7) / 8) or int(length / 8).
  */
  public static BitSet deserializeBit(int meta, ByteBuf buf) throws IOException {
    int bitSetLength = (meta >> 8) * 8 + (meta & 0xFF);
    return Utils.readBitSet(buf, bitSetLength);
  }
  // TINY
  public static Integer deserializeTiny(ByteBuf buf) {
    return new Integer(buf.readByte()); // TODO signed ??
  }

  // SHORT
  public static Integer deserializeShort(ByteBuf buf) {
    return new Integer(buf.readShortLE()); // TODO signed ??
  }

  // INT24
  public static Integer deserializeInt24(ByteBuf buf) {
    int result = 0; // TODO if signed is going to be wrong !!
    result |= buf.readUnsignedByte();
    result |= (((int) buf.readUnsignedByte()) << 8);
    result |= (((int) buf.readUnsignedByte()) << 16);

    return result;
  }

  // LONG
  public static Long deserializeLong(ByteBuf buf) {
    return buf.readUnsignedIntLE(); // TODO signedness
  }

  // LONGLONG
  public static Long deserializeLongLong(ByteBuf buf) {
    return buf.readLongLE(); // TODO signedness
  }

  // FLOAT
  public static Float deserializeFloat(ByteBuf buf) {
    return Float.intBitsToFloat(buf.readIntLE());
  }

  // DOUBLE
  public static Double deserializeDouble(ByteBuf buf) {
    return Double.longBitsToDouble(buf.readIntLE());
  }

  private static int bigEndianInteger(byte[] bytes, int offset, int length) {
    int result = 0;
    for (int i = offset; i < (offset + length); i++) {
      byte b = bytes[i];
      result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
    }
    return result;
  }

  /** see mysql/strings/decimal.c */
  private static BigDecimal asBigDecimal(int precision, int scale, byte[] value) {
    boolean positive = (value[0] & 0x80) == 0x80;
    value[0] ^= 0x80;
    if (!positive) {
      for (int i = 0; i < value.length; i++) {
        value[i] ^= 0xFF;
      }
    }
    int x = precision - scale;
    int ipDigits = x / DIG_PER_DEC;
    int ipDigitsX = x - ipDigits * DIG_PER_DEC;
    int ipSize = (ipDigits << 2) + DIG_TO_BYTES[ipDigitsX];
    int offset = DIG_TO_BYTES[ipDigitsX];
    BigDecimal ip =
        offset > 0 ? BigDecimal.valueOf(bigEndianInteger(value, 0, offset)) : BigDecimal.ZERO;
    for (; offset < ipSize; offset += 4) {
      int i = bigEndianInteger(value, offset, 4);
      ip = ip.movePointRight(DIG_PER_DEC).add(BigDecimal.valueOf(i));
    }
    int shift = 0;
    BigDecimal fp = BigDecimal.ZERO;
    for (; shift + DIG_PER_DEC <= scale; shift += DIG_PER_DEC, offset += 4) {
      int i = bigEndianInteger(value, offset, 4);
      fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIG_PER_DEC));
    }
    if (shift < scale) {
      int i = bigEndianInteger(value, offset, DIG_TO_BYTES[scale - shift]);
      fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
    }
    BigDecimal result = ip.add(fp);
    return positive ? result : result.negate();
  }

  /*
  A 1 byte unsigned int representing the precision,
  followed by a 1 byte unsigned int representing the number of decimals.
  */
  // NEWDECIMAL
  public static BigDecimal deserializeNewDecimal(int meta, ByteBuf buf) {
    int precision = meta & 0xFF, scale = meta >> 8, x = precision - scale;
    int ipd = x / DIG_PER_DEC, fpd = scale / DIG_PER_DEC;
    int decimalLength =
        (ipd << 2)
            + DIG_TO_BYTES[x - ipd * DIG_PER_DEC]
            + (fpd << 2)
            + DIG_TO_BYTES[scale - fpd * DIG_PER_DEC];
    byte[] content = new byte[decimalLength];
    buf.readBytes(content);
    return asBigDecimal(precision, scale, content);
  }

  /*
  private Long castTimestamp(Long timestamp, int fsp) {
    if (microsecondsPrecision && timestamp != null && !timestamp.equals(invalidDateAndTimeRepresentation)) {
      return timestamp * 1000 + fsp % 1000;
    }
    return timestamp;
  }
  */

  private static Long asUnixTime(
      int year, int month, int day, int hour, int minute, int second, int millis) {
    // https://dev.mysql.com/doc/refman/5.0/en/datetime.html
    /*if (year == 0 || month == 0 || day == 0) {
      return invalidDateAndTimeRepresentation;
    }*/
    return UnixTime.from(year, month, day, hour, minute, second, millis);
  }

  // DATE
  public static Date deserializeDate(ByteBuf buf) {
    int value = 0;
    value |= buf.readUnsignedByte();
    value |= (((int) buf.readUnsignedByte()) << 8);
    value |= (((int) buf.readUnsignedByte()) << 16);

    int day = value % 32;
    value >>>= 5;
    int month = value % 16;
    int year = value >> 4;
    Long timestamp = asUnixTime(year, month, day, 0, 0, 0, 0);

    return new Date(timestamp);
  }

  private static int[] split(long value, int divider, int length) {
    int[] result = new int[length];
    for (int i = 0; i < length - 1; i++) {
      result[i] = (int) (value % divider);
      value /= divider;
    }
    result[length - 1] = (int) value;
    return result;
  }

  // TIME
  public static Time deserializeTime(ByteBuf buf) {
    int value = 0;
    value |= buf.readUnsignedByte();
    value |= (((int) buf.readUnsignedByte()) << 8);
    value |= (((int) buf.readUnsignedByte()) << 16);

    int[] split = split(value, 100, 3);
    Long timestamp = asUnixTime(1970, 1, 1, split[2], split[1], split[0], 0);

    return new java.sql.Time(timestamp);
  }

  private static long bigEndianLong(byte[] bytes, int offset, int length) {
    long result = 0;
    for (int i = offset; i < (offset + length); i++) {
      byte b = bytes[i];
      result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
    }
    return result;
  }

  private static int deserializeFractionalSeconds(int meta, ByteBuf buf) {
    int length = (meta + 1) / 2;
    if (length > 0) {
      byte[] input = new byte[length];
      buf.readBytes(input);
      int fraction = bigEndianInteger(input, 0, length);
      return fraction * (int) Math.pow(100, 3 - length);
    }
    return 0;
  }

  private static int bitSlice(long value, int bitOffset, int numberOfBits, int payloadSize) {
    long result = value >> payloadSize - (bitOffset + numberOfBits);
    return (int) (result & ((1 << numberOfBits) - 1));
  }

  // TIMEV2
  public static Time deserializeTimeV2(int meta, ByteBuf buf) {
    /*
        (in big endian)

        1 bit sign (1= non-negative, 0= negative)
        1 bit unused (reserved for future extensions)
        10 bits hour (0-838)
        6 bits minute (0-59)
        6 bits second (0-59)

        (3 bytes in total)

        + fractional-seconds storage (size depends on meta)
    */
    byte[] input = new byte[3];
    buf.readBytes(input);

    long time = bigEndianLong(input, 0, 3);
    int fsp = deserializeFractionalSeconds(meta, buf);
    Long timestamp =
        asUnixTime(
            1970,
            1,
            1,
            bitSlice(time, 2, 10, 24),
            bitSlice(time, 12, 6, 24),
            bitSlice(time, 18, 6, 24),
            fsp / 1000);

    return new java.sql.Time(timestamp);
  }

  // TIMESTAMP
  public static Timestamp deserializeTimestamp(ByteBuf buf) {
    long timestamp = buf.readUnsignedIntLE() * 1000;
    return new java.sql.Timestamp(timestamp);
  }

  // TIMESTAMP_V2
  public static Timestamp deserializeTimestampV2(int meta, ByteBuf buf) {
    long millis = buf.readUnsignedInt();

    int fsp = deserializeFractionalSeconds(meta, buf);
    long timestamp = millis * 1000 + fsp / 1000;

    return new java.sql.Timestamp(timestamp);
  }

  // DATETIME
  public static java.util.Date deserializeDatetime(ByteBuf buf) throws IOException {
    long input = buf.readLongLE();
    int[] split = split(input, 100, 6);
    Long timestamp = asUnixTime(split[5], split[4], split[3], split[2], split[1], split[0], 0);
    return new java.util.Date(timestamp);
  }

  // DATETIME_V2
  public static java.util.Date deserializeDatetimeV2(int meta, ByteBuf buf) {
    /*
        (in big endian)

        1 bit sign (1= non-negative, 0= negative)
        17 bits year*13+month (year 0-9999, month 0-12)
        5 bits day (0-31)
        5 bits hour (0-23)
        6 bits minute (0-59)
        6 bits second (0-59)

        (5 bytes in total)

        + fractional-seconds storage (size depends on meta)
    */
    byte[] input = new byte[5];
    buf.readBytes(input);
    long datetime = bigEndianLong(input, 0, 5);
    int yearMonth = bitSlice(datetime, 1, 17, 40);
    int fsp = deserializeFractionalSeconds(meta, buf);
    Long timestamp =
        asUnixTime(
            yearMonth / 13,
            yearMonth % 13,
            bitSlice(datetime, 18, 5, 40),
            bitSlice(datetime, 23, 5, 40),
            bitSlice(datetime, 28, 6, 40),
            bitSlice(datetime, 34, 6, 40),
            fsp / 1000);
    return new java.util.Date(timestamp);
  }

  // YEAR
  public static Integer deserializeYear(ByteBuf buf) {
    return 1900 + buf.readUnsignedByte();
  }

  // STRING
  public static String deserializeString(int length, ByteBuf buf) {
    // TODO seguro??
    // charset is not present in the binary log (meaning there is no way to distinguish between CHAR
    // / BINARY)
    // as a result - return byte[] instead of an actual String
    int stringLength = length < 256 ? buf.readUnsignedByte() : buf.readUnsignedShortLE();

    // TODO ¿no se puede utilizar charset?
    return CodecUtils.readFixedLengthString(buf, stringLength, Charset.defaultCharset());
  }

  // VARCHAR
  // VAR_STRING
  public static String deserializeVarString(int meta, ByteBuf buf) {
    int stringLength = meta < 256 ? buf.readUnsignedByte() : buf.readUnsignedShortLE();
    // TODO ¿no se puede utilizar charset?
    return CodecUtils.readFixedLengthString(buf, stringLength, Charset.defaultCharset());
  }

  // BLOB
  public static byte[] deserializeBlob(int meta, ByteBuf buf) {
    int blobLength = readInteger(meta, buf);
    byte[] res = new byte[blobLength];
    buf.readBytes(res);
    return res;
  }

  /** Read int written in little-endian format. */
  private static int readInteger(int length, ByteBuf buf) {
    int result = 0;
    for (int i = 0; i < length; ++i) {
      result |= (buf.readUnsignedByte() << (i << 3));
    }
    return result;
  }

  // ENUM
  // TODO Apparently ENUM type can't be in the binlog
  // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html#Table_table_map_event_column_types
  public static Integer deserializeEnum(int length, ByteBuf buf) {
    // return readInteger(length, buf);
    throw new IllegalStateException("ENUM type can't be in the binlog");
  }

  // SET
  // TODO Apparently SET can't be in the binlog
  public static Long deserializeSet(int length, ByteBuf buf) throws IOException {
    // return inputStream.readLong(length);
    throw new IllegalStateException("SET type can't be in the binlog");
  }

  // GEOMETRY
  public static byte[] deserializeGeometry(int meta, ByteBuf buf) {
    int dataLength = readInteger(meta, buf);
    byte[] res = new byte[dataLength];
    buf.readBytes(res);
    return res;
  }

  /*
   * Deserialize the {@code JSON} value on the input stream, and return MySQL's internal binary
   * representation of the JSON value. See {@link
   * com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary} for a utility to parse
   * this binary representation into something more useful, including a string representation.
   */
  // JSON
  public static byte[] deserializeJson(int meta, ByteBuf buf) {
    int blobLength = readInteger(meta, buf);
    byte[] res = new byte[blobLength];
    buf.readBytes(res);
    return res;
  }

  /** Class for working with Unix time. */
  static class UnixTime {

    private static final int[] YEAR_DAYS_BY_MONTH =
        new int[] {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
    private static final int[] LEAP_YEAR_DAYS_BY_MONTH =
        new int[] {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

    /**
     * Calendar::getTimeInMillis but magnitude faster for all dates starting from October 15, 1582
     * (Gregorian Calendar cutover).
     *
     * @param year year
     * @param month month [1..12]
     * @param day day [1..)
     * @param hour hour [0..23]
     * @param minute [0..59]
     * @param second [0..59]
     * @param millis [0..999]
     * @return Unix time (number of seconds that have elapsed since 00:00:00 (UTC), Thursday, 1
     *     January 1970, not counting leap seconds)
     */
    // checkstyle, please ignore ParameterNumber for the next line
    public static long from(
        int year, int month, int day, int hour, int minute, int second, int millis) {
      if (year < 1582 || (year == 1582 && (month < 10 || (month == 10 && day < 15)))) {
        return fallbackToGC(year, month, day, hour, minute, second, millis);
      }
      long timestamp = 0;
      int numberOfLeapYears = leapYears(1970, year);
      timestamp += 366L * 24 * 60 * 60 * numberOfLeapYears;
      timestamp += 365L * 24 * 60 * 60 * (year - 1970 - numberOfLeapYears);
      long daysUpToMonth =
          isLeapYear(year) ? LEAP_YEAR_DAYS_BY_MONTH[month - 1] : YEAR_DAYS_BY_MONTH[month - 1];
      timestamp +=
          ((daysUpToMonth + day - 1) * 24 * 60 * 60) + (hour * 60 * 60) + (minute * 60) + (second);
      timestamp = timestamp * 1000 + millis;
      return timestamp;
    }

    // checkstyle, please ignore ParameterNumber for the next line
    private static long fallbackToGC(
        int year, int month, int dayOfMonth, int hourOfDay, int minute, int second, int millis) {
      Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      c.set(Calendar.YEAR, year);
      c.set(Calendar.MONTH, month - 1);
      c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
      c.set(Calendar.HOUR_OF_DAY, hourOfDay);
      c.set(Calendar.MINUTE, minute);
      c.set(Calendar.SECOND, second);
      c.set(Calendar.MILLISECOND, millis);
      return c.getTimeInMillis();
    }

    private static int leapYears(int from, int end) {
      return leapYearsBefore(end) - leapYearsBefore(from + 1);
    }

    private static int leapYearsBefore(int year) {
      year--;
      return (year / 4) - (year / 100) + (year / 400);
    }

    private static boolean isLeapYear(int year) {
      return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    }
  }
}

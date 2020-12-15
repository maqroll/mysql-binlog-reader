package com.github.maqroll;

import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import org.junit.jupiter.api.Test;

/**
 * TODO: we need test comparing the result from this decoder and the old one for ALL datatypes and
 * extreme cases (limits, signed, etc...)
 */
public class DeserializerTest {

  @Test
  public void deserializeBit() {

    /*
      public Serializable deserializeBit(int meta, ByteArrayInputStream inputStream) throws IOException {
      int bitSetLength = (meta >> 8) * 8 + (meta & 0xFF);
      return inputStream.readBitSet(bitSetLength, false);
    }*/
    WriteRowsEventDataDeserializer deserializer = new WriteRowsEventDataDeserializer(null);
  }
}

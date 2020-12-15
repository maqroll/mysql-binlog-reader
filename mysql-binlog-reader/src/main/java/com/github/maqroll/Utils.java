package com.github.maqroll;

import io.netty.buffer.ByteBuf;
import java.util.BitSet;

public class Utils {
  private Utils() {}

  public static BitSet readBitSet(ByteBuf buf, int bits) {
    int bytesSize = (int) ((bits + 7) / 8);
    byte[] bytes = new byte[bytesSize];
    buf.readBytes(bytes);
    return BitSet.valueOf(bytes);
  }
}

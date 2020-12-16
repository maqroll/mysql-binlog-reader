package com.github.maqroll;

import io.netty.buffer.ByteBuf;
import java.util.BitSet;

public class Utils {
  private Utils() {}

  public static BitSet readBitSet(ByteBuf buf, int bits) {
    int bytesSize = (int) ((bits + 7) / 8);
    byte[] bytes = new byte[bytesSize];
    buf.readBytes(bytes);

    // clear
    BitSet clear = new BitSet(bits);
    for (int i = 0; i < bits; i++) {
      clear.set(i);
    }

    BitSet res = BitSet.valueOf(bytes);
    res.and(clear);
    return res;
  }
}

package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Position;
import java.util.Objects;

public class ROPositionImpl implements Position {
  private String fileName;
  private long pos;

  public ROPositionImpl() {
    this.fileName = null;
    this.pos = 0;
  }

  public ROPositionImpl(String fileName, long pos) {
    Objects.requireNonNull(fileName, "can't init Position with null filename");
    this.fileName = fileName;
    this.pos = pos;
  }

  public void update(String fileName, long pos) {
    throw new IllegalStateException("ROPosition not updatable");
  }

  public String getFileName() {
    return fileName;
  }

  public long getPos() {
    return pos;
  }
}

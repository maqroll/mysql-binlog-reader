package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Position;
import java.util.Objects;

public class PositionImpl implements Position {
  private String fileName;
  private long pos;

  public PositionImpl() {
    this.fileName = null;
    this.pos = 0;
  }

  public PositionImpl(String fileName, long pos) {
    Objects.requireNonNull(fileName, "can't init Position with null filename");
    this.fileName = fileName;
    this.pos = pos;
  }

  public void update(String fileName, long pos) {
    Objects.requireNonNull(fileName, "can't update Position with null filename");
    this.fileName = fileName;
    this.pos = pos;
  }

  public String getFileName() {
    return fileName;
  }

  public long getPos() {
    return pos;
  }
}

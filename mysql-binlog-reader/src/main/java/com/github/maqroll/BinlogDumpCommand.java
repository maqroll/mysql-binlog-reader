package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Command;
import com.github.mheath.netty.codec.mysql.CommandPacket;

import java.util.Collections;
import java.util.EnumSet;

public class BinlogDumpCommand extends CommandPacket {

  private final EnumSet<BinlogDumpFlag> flags;
  private final int pos;
  private final int serverId;
  private final String fileName;

  private BinlogDumpCommand(Builder builder) {
    super(builder.sequenceId, Command.COM_BINLOG_DUMP);
    this.flags = builder.flags;
    this.pos = builder.pos;
    this.serverId = builder.serverId;
    this.fileName = builder.fileName;
  }

  public EnumSet<BinlogDumpFlag> getFlags() {
    return flags;
  }

  public int getPos() {
    return pos;
  }

  public int getServerId() {
    return serverId;
  }

  public String getFileName() {
    return fileName;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int sequenceId;
    private EnumSet<BinlogDumpFlag> flags = EnumSet.noneOf(BinlogDumpFlag.class);
    private int pos;
    private String fileName;
    private int serverId;

    public Builder sequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
      return this;
    }

    public Builder pos(int pos) {
      this.pos = pos;
      return this;
    }
    
    public Builder serverId(int serverId) {
      this.serverId = serverId;
      return this;
    }

    public Builder fileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    // oversize
    public Builder addFlags(BinlogDumpFlag flag, BinlogDumpFlag... flags) {
      this.flags.add(flag);
      Collections.addAll(this.flags, flags);
      return this;
    }

    public BinlogDumpCommand build() {
      // TODO validaciones
      if (pos < 4) { pos = 4; }
      return new BinlogDumpCommand(this);
    }
  }
}

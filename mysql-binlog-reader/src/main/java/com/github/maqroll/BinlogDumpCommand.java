package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Command;
import com.github.mheath.netty.codec.mysql.CommandPacket;

import java.util.EnumSet;

public class BinlogDumpCommand extends CommandPacket {

  private final EnumSet<BinlogDumpFlag> flags;
  private final long pos;
  private final long serverId;
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

  public long getPos() {
    return pos;
  }

  public long getServerId() {
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
    private EnumSet<BinlogDumpFlag> flags;
    private Long pos;
    private String fileName;
    private Long serverId;

    public Builder sequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
      return this;
    }

    public Builder pos(long pos) {
      this.pos = pos;
      return this;
    }
    
    public Builder serverId(long serverId) {
      this.serverId = serverId;
      return this;
    }

    public Builder fileName(String fileName) {
      this.fileName = fileName;
      return this;
    }
    
    public BinlogDumpCommand build() {
      // TODO validaciones
      return new BinlogDumpCommand(this);
    }
  }
}
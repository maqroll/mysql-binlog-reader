package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;

public class WriteRowsEventPayload implements ReplicationEventPayload {
  // TODO

  private WriteRowsEventPayload(Builder builder) {
    // TODO
  }

  public static class Builder {
    // TODO

    public WriteRowsEventPayload build() {
      return new WriteRowsEventPayload(this);
    }

    public Builder x() {
      return this;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  // TODO
}

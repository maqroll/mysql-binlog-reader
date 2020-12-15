package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitable;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitor;

public class WriteRowsEventPayload implements ReplicationEventPayload, RowsChangedVisitable {
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

  @Override
  public void accept(RowsChangedVisitor visitor) {
    // TODO
  }
}

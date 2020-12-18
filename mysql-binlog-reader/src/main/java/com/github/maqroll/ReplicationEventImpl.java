package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ReplicationEvent;
import com.github.mheath.netty.codec.mysql.ReplicationEventHeader;
import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;

public class ReplicationEventImpl implements ReplicationEvent {
  private final ReplicationEventPayload payload;
  private final ReplicationEventHeader header;

  public ReplicationEventImpl(ReplicationEventHeader header, ReplicationEventPayload payload) {
    this.header = header;
    this.payload = payload;
  }

  @Override
  public ReplicationEventHeader header() {
    return header;
  }

  @Override
  public ReplicationEventPayload payload() {
    return payload;
  }
}

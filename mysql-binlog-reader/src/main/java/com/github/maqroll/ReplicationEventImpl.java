package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Position;
import com.github.mheath.netty.codec.mysql.ReplicationEvent;
import com.github.mheath.netty.codec.mysql.ReplicationEventHeader;
import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;

public class ReplicationEventImpl implements ReplicationEvent {
  private final ReplicationEventPayload payload;
  private final ReplicationEventHeader header;
  private final Position next;

  public ReplicationEventImpl(
      ReplicationEventHeader header, ReplicationEventPayload payload, Position next) {
    this.header = header;
    this.payload = payload;
    this.next = next;
  }

  @Override
  public ReplicationEventHeader header() {
    return header;
  }

  @Override
  public ReplicationEventPayload payload() {
    return payload;
  }

  @Override
  public Position next() {
    return next;
  }
}

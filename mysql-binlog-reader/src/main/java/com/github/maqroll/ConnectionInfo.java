package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Position;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class ConnectionInfo {
  private static final AttributeKey<ConnectionInfo> key =
      AttributeKey.newInstance(ConnectionInfo.class.getName());
  private final Endpoint endpoint;
  private ChecksumType checksumType = null;
  private Position pos = null;
  private final boolean stopAtEOF;

  public ConnectionInfo(Endpoint endpoint, boolean stopAtEOF) {
    this.endpoint = endpoint;
    this.stopAtEOF = stopAtEOF;
  }

  // discovered later
  public void setChecksumType(ChecksumType checksumType) {
    this.checksumType = checksumType;
  }

  public void updatePosition(Position pos) {
    this.pos = pos;
  }

  public boolean stopAtEOF() {
    return stopAtEOF;
  }

  public Position getPosition() {
    return this.pos;
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  public ChecksumType getChecksumType() {
    return checksumType;
  }

  public static ConnectionInfo getCurrent(Channel channel) {
    final Attribute<ConnectionInfo> attr = channel.attr(key);
    if (attr.get() == null) {
      throw new IllegalStateException("Channel initialized without server info");
    }
    return attr.get();
  }

  public void setCurrent(Channel channel) {
    final Attribute<ConnectionInfo> attr = channel.attr(key);
    attr.set(this);
  }
}

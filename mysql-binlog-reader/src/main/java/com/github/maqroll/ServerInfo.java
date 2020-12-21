package com.github.maqroll;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class ServerInfo {
  private static final AttributeKey<ServerInfo> key =
      AttributeKey.newInstance(ServerInfo.class.getName());
  private final Endpoint endpoint;
  private ChecksumType checksumType = null;

  public ServerInfo(Endpoint endpoint) {
    this.endpoint = endpoint;
  }

  public void setChecksumType(ChecksumType checksumType) {
    this.setChecksumType(checksumType);
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  public ChecksumType getChecksumType() {
    return checksumType;
  }

  public static ServerInfo getCurrent(Channel channel) {
    final Attribute<ServerInfo> attr = channel.attr(key);
    if (attr.get() == null) {
      throw new IllegalStateException("Channel initialized without server info");
    }
    return attr.get();
  }

  public void setCurrent(Channel channel) {
    final Attribute<ServerInfo> attr = channel.attr(key);
    attr.set(this);
  }
}

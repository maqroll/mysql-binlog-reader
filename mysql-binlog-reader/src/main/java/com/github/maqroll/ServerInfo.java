package com.github.maqroll;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class ServerInfo {
  private static final AttributeKey<ServerInfo> key =
      AttributeKey.newInstance(ServerInfo.class.getName());
  private final BinlogClient client;
  private ChecksumType checksumType;

  public ServerInfo(BinlogClient client, ChecksumType checksumType) {
    this.client = client;
    this.checksumType = checksumType;
  }

  public void setChecksumType(ChecksumType checksumType) {
    this.setChecksumType(checksumType);
  }

  public BinlogClient getClient() {
    return client;
  }

  public ChecksumType getChecksumType() {
    return checksumType;
  }

  public static ServerInfo getServerInfoAttr(Channel channel) {
    final Attribute<ServerInfo> attr = channel.attr(key);
    if (attr.get() == null) {
      throw new IllegalStateException("Channel initialized without server info");
    }
    return attr.get();
  }

  public static void setServerInfoAttr(Channel channel, ServerInfo serverInfo) {
    final Attribute<ServerInfo> attr = channel.attr(key);
    attr.set(serverInfo);
  }
}

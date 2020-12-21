package com.github.maqroll;

import java.util.Objects;

/**
 * If there is only one client, go for blocking connect() autocloseable. If there are more than one
 * client, getting binlog from many clients it's better to handle all the connections in one place.
 * How? A ClientHub class that allows to wait for all, stop if any, etc.... and reuse threads.
 */
public class BinlogClient {
  private final Endpoint endpoint;

  public static class Builder {

    private final String host;
    private final int port;
    private final String user;
    private final String password;

    public Builder(String host, int port, String user, String password) {
      Objects.requireNonNull(host, "client host can't be null");
      if (port <= 0) throw new IllegalArgumentException("client port should be greater than 0");
      Objects.requireNonNull(user, "client user can't be null");
      Objects.requireNonNull(password, "client password can't be null");
      this.host = host;
      this.port = port;
      this.user = user;
      this.password = password;
    }

    public BinlogClient build() {
      return new BinlogClient(this);
    }
  }

  private BinlogClient(Builder builder) {
    endpoint = new Endpoint(builder.host, builder.port, builder.user, builder.password);
  }

  public void connect() {
    // TODO no blocking
  }

  public void waitUntilClosed() {
    // TODO
  }
}

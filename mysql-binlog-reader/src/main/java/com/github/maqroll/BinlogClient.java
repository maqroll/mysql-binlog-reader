package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Position;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitor;
import java.util.Objects;

/**
 * If there is only one client, go for blocking connect() autocloseable. If there are more than one
 * client, getting binlog from many clients it's better to handle all the connections in one place.
 * How? A ClientHub class that allows to wait for all, stop if any, etc.... and reuse threads.
 */
public class BinlogClient {
  private final Endpoint endpoint;
  private final Position init;
  private final RowsChangedVisitor rowChangesVisitor;
  private BinlogConnection conn;
  private final boolean stopAtEOF;

  public static class Builder {

    private final String host;
    private final int port;
    private final String user;
    private final String password;

    private Position init;
    private RowsChangedVisitor rowChangesVisitor;
    private boolean stopAtEOF = false;

    /*
    By default starts from the beginning and don't stop at EOF.
     */
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

    public Builder from(String fileName, int pos) {
      this.init = new ROPositionImpl(fileName, pos);
      return this;
    }

    public Builder stopAtEOF() {
      this.stopAtEOF = true;
      return this;
    }

    public Builder setRowsChangesVisitor(RowsChangedVisitor visitor) {
      this.rowChangesVisitor = visitor;
      return this;
    }

    public BinlogClient build() {
      return new BinlogClient(this);
    }
  }

  private BinlogClient(Builder builder) {
    endpoint = new Endpoint(builder.host, builder.port, builder.user, builder.password);
    init = builder.init;
    rowChangesVisitor = builder.rowChangesVisitor;
    stopAtEOF = builder.stopAtEOF;
  }

  public static Builder builder(String host, int port, String user, String password) {
    return new Builder(host, port, user, password);
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  public Position getInit() {
    return init;
  }

  public boolean stopAtEOF() {
    return stopAtEOF;
  }

  public RowsChangedVisitor getVisitor() {
    return rowChangesVisitor;
  }

  public void connect() {
    // TODO inject lifecycle listener
    if (conn == null || !conn.isConnected()) { // nothing if already connected
      conn = new BinlogConnection(this);
    }
  }

  public void waitUntilClosed() {
    conn.waitUntilClosed();
  }
}

package com.github.maqroll;

public class Endpoint {
  private final String host;
  private final int port;
  private final String user;
  private final String password;

  public Endpoint(String host, int port, String user, String password) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }
}

package com.github.maqroll;

import org.junit.jupiter.api.Test;

public class AppTest {
  static int serverPort = /*Integer.parseInt(System.getProperty("mysql1.port"))*/ 32778;

  @Test
  public void shouldAnswerWithTrue() {
    new BinlogConnection(serverPort);
  }
}

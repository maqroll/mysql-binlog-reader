package com.github.maqroll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AppTest extends BaseTest {

  @BeforeAll
  public static void setup() throws SQLException {
    try (Connection conn = DriverManager.getConnection(LOCAL_URL, ROOT_USER, ROOT_PWD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(GRANT_REPLICATION_PRIVILEGES);
        stmt.execute(INSTALL_BLACKHOLE_PLUGIN);
        stmt.execute(DROP_TABLE_IF_EXISTS);
        stmt.execute(CREATE_TABLE);
        stmt.execute(CLEAN_LOGS);
        stmt.execute(INSERT);
        stmt.execute(UPDATE);
        stmt.execute(DELETE);
      }
    }
  }

  @Test
  public void basicTest() {
    BinlogClient.Builder builder =
        BinlogClient.builder(HOST, PORT, REPLICATION_USER, REPLICATION_PWD);
    builder.stopAtEOF();
    BinlogClient client = builder.build();
    
    client.connect();
    client.waitUntilClosed();
  }
}

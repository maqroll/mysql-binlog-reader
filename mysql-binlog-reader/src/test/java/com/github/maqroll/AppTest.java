package com.github.maqroll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AppTest {
  private static final String HOST = "localhost";
  private static final int PORT = /*Integer.parseInt(System.getProperty("mysql1.port"))*/ 55000;
  private static final String DB = "test_db";
  private static final String TBL = "table1";
  private static final String LOCAL_URL = "jdbc:mariadb://" + HOST + ":" + PORT + "/" + DB;
  private static final String ROOT_USER = "root";
  private static final String ROOT_PWD = "root";
  private static final String REPLICATION_USER = "db_user";
  private final String REPLICATION_PWD = "password";
  private static final String GRANT_REPLICATION_PRIVILEGES =
      "GRANT SELECT, RELOAD, REPLICATION SLAVE, BINLOG MONITOR  ON *.* TO '"
          + REPLICATION_USER
          + "'";
  private static final String INSTALL_BLACKHOLE_PLUGIN = "INSTALL SONAME 'ha_blackhole'";
  private static final String CLEAN_LOGS = "RESET MASTER";
  private static final String DROP_TABLE_IF_EXISTS = "DROP TABLE IF EXISTS " + TBL;
  private static final String CREATE_TABLE =
      "CREATE TABLE " + TBL + " (a int, b int DEFAULT 3) ENGINE=BLACKHOLE";
  private static final String INSERT = "INSERT INTO " + TBL + "(a) values(5)";
  private static final String GET_CHECKSUM = "show global variables like 'binlog_checksum'";

  @BeforeAll
  public static void setup() throws SQLException {
    try (Connection conn = DriverManager.getConnection(LOCAL_URL, ROOT_USER, ROOT_PWD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(GRANT_REPLICATION_PRIVILEGES);
        stmt.execute(INSTALL_BLACKHOLE_PLUGIN);
        stmt.execute(DROP_TABLE_IF_EXISTS);
        stmt.execute(CREATE_TABLE);
        stmt.execute(CLEAN_LOGS);
        for (int i = 0; i < 10; i++) {
          stmt.execute(INSERT);
        }
      }
    }
  }

  @Test
  public void shouldAnswerWithTrue() {
    new BinlogConnection(new Endpoint("localhost", 54, "user", "password"));
  }
}

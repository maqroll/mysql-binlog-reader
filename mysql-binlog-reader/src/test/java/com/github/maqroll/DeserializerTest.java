package com.github.maqroll;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * TODO: we need test comparing the result from this decoder and the old one for ALL datatypes and
 * extreme cases (limits, signed, etc...)
 */
public class DeserializerTest extends BaseTest {
  static final String CREATE_TABLE_BIT = "CREATE TABLE " + TBL + " (a bit)";
  static final String INSERT_BIT_1 = "INSERT INTO " + TBL + " VALUES (b'1')";
  static final String INSERT_BIT_0 = "INSERT INTO " + TBL + " VALUES (b'0')";

  @BeforeAll
  public static void setupReplicationPrivileges() throws SQLException {
    try (Connection conn = DriverManager.getConnection(LOCAL_URL, ROOT_USER, ROOT_PWD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(GRANT_REPLICATION_PRIVILEGES);
        stmt.execute(INSTALL_BLACKHOLE_PLUGIN);
      }
    }
  }

  @BeforeEach
  public void setupEach() throws SQLException {
    try (Connection conn = DriverManager.getConnection(LOCAL_URL, ROOT_USER, ROOT_PWD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(CREATE_OR_REPLACE_SCHEMA);
        stmt.execute(CLEAN_LOGS);
      }
    }
  }

  private static void execOnDB(String... cmds) throws SQLException {
    try (Connection conn = DriverManager.getConnection(LOCAL_URL, ROOT_USER, ROOT_PWD)) {
      try (Statement stmt = conn.createStatement()) {
        for( String cmd : cmds) {
          stmt.execute(cmd);
        }
      }
    }
  }

  // TODO metodos auxiliares para
  // -- leer el binlog con cada parser (en cada test)
  // -- comparar el resultado

  @Test
  public void deserializeBit_0() throws SQLException {
    execOnDB(CREATE_TABLE_BIT, INSERT_BIT_0);
  }

  @Test
  public void deserializeBit_1() throws SQLException {
    execOnDB(CREATE_TABLE_BIT, INSERT_BIT_1);
  }
}

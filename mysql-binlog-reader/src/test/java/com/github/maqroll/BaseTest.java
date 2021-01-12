package com.github.maqroll;

public class BaseTest {
  static final String HOST = "localhost";
  static final int PORT = /*Integer.parseInt(System.getProperty("mysql1.port"))*/ 55010;
  static final String DB = "test_db";
  static final String TBL = "table1";
  static final String LOCAL_URL = "jdbc:mariadb://" + HOST + ":" + PORT + "/" + DB;
  static final String ROOT_USER = "root";
  static final String ROOT_PWD = "root";
  static final String REPLICATION_USER = "db_user";
  static final String REPLICATION_PWD = "password";
  static final String GRANT_REPLICATION_PRIVILEGES =
      "GRANT SELECT, RELOAD, REPLICATION SLAVE, BINLOG MONITOR  ON *.* TO '"
          + REPLICATION_USER
          + "'";
  static final String INSTALL_BLACKHOLE_PLUGIN = "INSTALL SONAME 'ha_blackhole'";
  static final String CLEAN_LOGS = "RESET MASTER";
  static final String DROP_TABLE_IF_EXISTS = "DROP TABLE IF EXISTS " + TBL;
  static final String CREATE_TABLE = "CREATE TABLE " + TBL + " (a int, b int DEFAULT 3)";
  static final String INSERT = "INSERT INTO " + TBL + "(a) values(5)";
  static final String DELETE = "DELETE FROM " + TBL;
  static final String UPDATE = "UPDATE " + TBL + " SET a=a+1";
  static final String CREATE_OR_REPLACE_SCHEMA = "CREATE OR REPLACE DATABASE " + DB;
  static final String GET_CHECKSUM = "show global variables like 'binlog_checksum'";
}

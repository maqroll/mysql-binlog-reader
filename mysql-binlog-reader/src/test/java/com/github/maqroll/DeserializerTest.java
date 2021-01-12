package com.github.maqroll;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.mheath.netty.codec.mysql.ColumnType;
import com.github.mheath.netty.codec.mysql.Row;
import com.github.mheath.netty.codec.mysql.RowVisitor;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitor;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * TODO: we need test comparing the result from this decoder and the old one for ALL datatypes and
 * extreme cases (limits, signed, etc...)
 */
public class DeserializerTest extends BaseTest {
  static final String CREATE_TABLE_BIT = "CREATE TABLE " + TBL + " (a bit)";
  static final String INSERT_BIT_1 = "INSERT INTO " + TBL + " VALUES (b'1')";
  static final String INSERT_BIT_0 = "INSERT INTO " + TBL + " VALUES (b'0')";

  class OneRowWriteEventListener implements BinaryLogClient.EventListener {
    private Serializable colValue = null;

    @Override
    public void onEvent(Event event) {
      final EventHeader header = event.getHeader();
      if (EventType.WRITE_ROWS.equals(header.getEventType())) {
        final WriteRowsEventData data = event.getData();
        final List<Serializable[]> rows = data.getRows();
        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).length);
        colValue = rows.get(0)[0];
      }
    }

    public Serializable getColValue() {
      return colValue;
    }
  }

  class OneColumnRowVisitor implements RowVisitor {
    Object value = null;

    @Override
    public void visit(int idx, ColumnType type) {
      /* TODO value = */
    }

    public Object getValue() {
      return value;
    }
  }

  class OneRowChangesVisitor implements RowsChangedVisitor {
    OneColumnRowVisitor visitor = new OneColumnRowVisitor();

    @Override
    public void added(String db, String table, Stream<Row> rows, boolean eos) {
      Optional<Row> first = rows.findFirst();

      assertTrue(first.isPresent());
      first.get().accept(visitor);
    }

    @Override
    public void removed(String db, String table, Stream<Row> rows, boolean eos) { }

    @Override
    public void updated(String db, String table, Stream<Row> rows, boolean eos) { }

    public Object getValue() {
      return visitor.getValue();
    }

  }

  @BeforeAll
  public static void setupReplicationPrivileges() throws SQLException {
    try (Connection conn = DriverManager.getConnection(LOCAL_URL, ROOT_USER, ROOT_PWD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(GRANT_REPLICATION_PRIVILEGES);
        stmt.execute(INSTALL_BLACKHOLE_PLUGIN);
      }
    }
  }

  private Serializable getWriteRowValueWithSyncReader() throws IOException {
    OneRowWriteEventListener evtListener = new OneRowWriteEventListener();

    BinaryLogClient binaryLogClient =
        new BinaryLogClient(HOST, PORT, REPLICATION_USER, REPLICATION_PWD);
    binaryLogClient.setEventDeserializer(new EventDeserializer());
    binaryLogClient.registerEventListener(evtListener);
    binaryLogClient.setBinlogFilename("");
    binaryLogClient.setBlocking(false);
    binaryLogClient.connect();

    return evtListener.getColValue();
  }

  private Object getWriteRowValueWithAsyncReader() {
    OneRowChangesVisitor visitor = new OneRowChangesVisitor();

    BinlogClient.Builder builder =
        BinlogClient.builder(HOST, PORT, REPLICATION_USER, REPLICATION_PWD);
    builder.stopAtEOF();
    builder.setRowsChangesVisitor(visitor);
    BinlogClient client = builder.build();
    client.connect();
    client.waitUntilClosed();

    return visitor.getValue();
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
        for (String cmd : cmds) {
          stmt.execute(cmd);
        }
      }
    }
  }

  // TODO metodos auxiliares para
  // -- comparar el resultado

  @Test
  public void deserializeBit_0() throws SQLException, IOException {
    execOnDB(CREATE_TABLE_BIT, INSERT_BIT_0);

    final Serializable oldValue = getWriteRowValueWithSyncReader();
    final Object newValue = getWriteRowValueWithAsyncReader();
  }

  @Test
  public void deserializeBit_1() throws SQLException, IOException {
    execOnDB(CREATE_TABLE_BIT, INSERT_BIT_1);

    final Serializable oldValue = getWriteRowValueWithSyncReader();
    final Object newValue = getWriteRowValueWithAsyncReader();
  }
}

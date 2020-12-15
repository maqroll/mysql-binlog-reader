package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ColumnType;
import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import java.util.BitSet;
import java.util.List;

public class TableMapEventPayload implements ReplicationEventPayload {
  private final long tableId;
  private final String database;
  private final String table;
  private final List<ColumnType> columnTypes;
  // private final int[] columnMetadata;
  private final BitSet columnNullability;
  // private TableMapEventMetadata eventMetadata;

  private TableMapEventPayload(Builder builder) {
    tableId = builder.tableId;
    database = builder.database;
    table = builder.table;
    columnTypes = builder.columnTypes;
    columnNullability = builder.columnNullability;
  }

  public static class Builder {
    private long tableId;
    private String database;
    private String table;
    private List<ColumnType> columnTypes;
    private int[] columnMetadata;
    private BitSet columnNullability;
    // private TableMapEventMetadata eventMetadata;

    public TableMapEventPayload build() {
      return new TableMapEventPayload(this);
    }

    public Builder tableId(long tableId) {
      this.tableId = tableId;
      return this;
    }

    public Builder database(String database) {
      this.database = database;
      return this;
    }

    public Builder table(String table) {
      this.table = table;
      return this;
    }

    public Builder columnTypes(List<ColumnType> columnTypes) {
      this.columnTypes = columnTypes;
      return this;
    }

    public Builder columnNullability(BitSet columnNullability) {
      this.columnNullability = columnNullability;
      return this;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public long getTableId() {
    return tableId;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public List<ColumnType> getColumnTypes() {
    return columnTypes;
  }

  public BitSet getColumnNullability() {
    return columnNullability;
  }
}

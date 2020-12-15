package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitable;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitor;
import java.util.BitSet;
import java.util.List;

public class WriteRowsEventPayload implements ReplicationEventPayload, RowsChangedVisitable {
  private TableMapEventPayload tableMap;
  private long columnCount;
  private BitSet columnsSent;
  private List<Object[]> rows; // TODO change for something more visitor-friendly

  private WriteRowsEventPayload(Builder builder) {
    tableMap = builder.tableMap;
    columnCount = builder.columnCount;
    columnsSent = builder.columnsSent;
    rows = builder.rows;
  }

  public static class Builder {
    private TableMapEventPayload tableMap;
    private long columnCount;
    private BitSet columnsSent;
    private List<Object[]> rows;

    public WriteRowsEventPayload build() {
      return new WriteRowsEventPayload(this);
    }

    public Builder tableMap(TableMapEventPayload tableMap) {
      this.tableMap = tableMap;
      return this;
    }

    public Builder columnCount(long columnCount) {
      this.columnCount = columnCount;
      return this;
    }

    public Builder columnsSent(BitSet columnsSent) {
      this.columnsSent = columnsSent;
      return this;
    }

    public Builder rows(List<Object[]> rows) {
      this.rows = rows;
      return this;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public TableMapEventPayload getTableMap() {
    return tableMap;
  }

  public long getColumnCount() {
    return columnCount;
  }

  public BitSet getColumnsSent() {
    return columnsSent;
  }

  public List<Object[]> getRows() {
    return rows;
  }

  @Override
  public void accept(RowsChangedVisitor visitor) {
    // TODO
  }
}

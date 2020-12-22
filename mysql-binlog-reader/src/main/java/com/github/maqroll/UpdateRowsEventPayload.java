package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import com.github.mheath.netty.codec.mysql.Row;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitable;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitor;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;

public class UpdateRowsEventPayload implements ReplicationEventPayload, RowsChangedVisitable {
  private final TableMapEventPayload tableMap;
  private final long columnCount;
  private final BitSet columnsSentBefore;
  private final BitSet columnsSentUpdate;
  private final List<Row /*Object[]*/>
      rowsBefore; // TODO change for something more visitor-friendly
  private final List<Row /*Object[]*/>
      rowsUpdate; // TODO change for something more visitor-friendly
  private final List<Integer> columnsPresentBefore;
  private final Stream<Row> rowStream;

  private UpdateRowsEventPayload(Builder builder) {
    tableMap = builder.tableMap;
    columnCount = builder.columnCount;
    columnsSentBefore = builder.columnsSentBefore;
    columnsSentUpdate = builder.columnsSentUpdate;
    rowsBefore = builder.rowsBefore;
    rowsUpdate = builder.rowsUpdate;

    columnsPresentBefore = new ArrayList<>();
    for (int c = 0; c < (int) columnCount; c++) { // FIXME ¿columnCount could be greater than int?
      if (columnsSentBefore.get(c)) {
        columnsPresentBefore.add(c);
      }
    }

    // TODO y si guardamos Row en lugar de Object[] en rows???
    // el stream no necesitaría mapear.
    rowStream = rowsBefore.stream();
  }

  public static class Builder {
    private TableMapEventPayload tableMap;
    private long columnCount;
    private BitSet columnsSentBefore;
    private BitSet columnsSentUpdate;
    private List<Row> rowsBefore;
    private List<Row> rowsUpdate;

    public UpdateRowsEventPayload build() {
      return new UpdateRowsEventPayload(this);
    }

    public Builder tableMap(TableMapEventPayload tableMap) {
      this.tableMap = tableMap;
      return this;
    }

    public Builder columnCount(long columnCount) {
      this.columnCount = columnCount;
      return this;
    }

    public Builder columnsSentBefore(BitSet columnsSent) {
      this.columnsSentBefore = columnsSent;
      return this;
    }

    public Builder columnsSentUpdate(BitSet columnsSent) {
      this.columnsSentUpdate = columnsSent;
      return this;
    }

    public Builder rowsBefore(List<Row /*Object[]*/> rows) {
      this.rowsBefore = rows;
      return this;
    }

    public Builder rowsUpdate(List<Row /*Object[]*/> rows) {
      this.rowsUpdate = rows;
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

  public BitSet getColumnsSentBefore() {
    return columnsSentBefore;
  }

  public List<Row /*Object[]*/> getRowsBefore() {
    return rowsBefore;
  }

  @Override
  public void accept(RowsChangedVisitor visitor) {
    visitor.updated(tableMap.getDatabase(), tableMap.getTable(), rowStream);
  }
}

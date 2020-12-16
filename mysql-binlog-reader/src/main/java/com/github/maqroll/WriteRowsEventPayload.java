package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ReplicationEventPayload;
import com.github.mheath.netty.codec.mysql.Row;
import com.github.mheath.netty.codec.mysql.RowVisitor;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitable;
import com.github.mheath.netty.codec.mysql.RowsChangedVisitor;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;

public class WriteRowsEventPayload implements ReplicationEventPayload, RowsChangedVisitable {
  private final TableMapEventPayload tableMap;
  private final long columnCount;
  private final BitSet columnsSent;
  private final List<Object[]> rows; // TODO change for something more visitor-friendly
  private final List<Integer> columnsPresent;
  private final Stream<Row> rowStream;

  private WriteRowsEventPayload(Builder builder) {
    tableMap = builder.tableMap;
    columnCount = builder.columnCount;
    columnsSent = builder.columnsSent;
    rows = builder.rows;

    columnsPresent = new ArrayList<>();
    for (int c = 0; c < (int) columnCount; c++) { // FIXME ¿columnCount could be greater than int?
      if (columnsSent.get(c)) {
        columnsPresent.add(c);
      }
    }

    rowStream =
        rows.stream()
            .map(
                row -> {
                  return new Row() {
                    @Override
                    public void accept(RowVisitor visitor) {
                      for (int i = 0; i < row.length; i++) {
                        visitor.visit(columnsPresent.get(i), tableMap.getColumnTypes().get(i));
                      }
                    }
                  };
                });
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
    visitor.added(tableMap.getDatabase(), tableMap.getTable(), rowStream);

    /*for (Object[] row : rows) {
      // FIXME
      for (int i=0; i < row.length; i++) {
        visitor.columnAddedRow(tableMap.getDatabase(), tableMap.getTable(), columnsPresent.get(i), tableMap.getColumnTypes().get(i));
      }
    }

    visitor.endAddedRow(tableMap.getDatabase(), tableMap.getTable()); // TODO commit pos ??
     */
  }
}

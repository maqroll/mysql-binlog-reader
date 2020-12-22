package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Row;
import com.github.mheath.netty.codec.mysql.RowVisitor;
import java.util.List;

public class RowUpdateImpl implements Row {
  private final Object[] valuesBefore;
  private final Object[] valuesUpdate;
  private final List<Integer> columnsPresentBefore;
  private final List<Integer> columnsPresentUpdate;
  private final TableMapEventPayload tableMap;

  public RowUpdateImpl(
      TableMapEventPayload tableMap,
      List<Integer> columnsPresentBefore,
      Object[] valuesBefore,
      List<Integer> columnsPresentUpdate,
      Object[] valuesUpdate) {
    this.tableMap = tableMap;
    this.columnsPresentBefore = columnsPresentBefore;
    this.columnsPresentUpdate = columnsPresentUpdate;
    this.valuesBefore = valuesBefore;
    this.valuesUpdate = valuesUpdate;
  }

  @Override
  public void accept(RowVisitor visitor) {
    int iBefore = 0, iAfter = 0;

    while (iBefore < valuesBefore.length || iAfter < valuesUpdate.length) {
      Integer columnBefore = columnsPresentBefore.get(iBefore);
      Integer columnAfter = columnsPresentUpdate.get(iAfter);

      if (columnAfter == columnAfter) {
        visitor.visit(columnBefore, tableMap.getColumnTypes().get(columnBefore));
        iBefore++;
        iAfter++;
      } else if (columnAfter < columnBefore) {
        visitor.visit(columnAfter, tableMap.getColumnTypes().get(columnAfter));
        iAfter++;
      } else {
        visitor.visit(columnBefore, tableMap.getColumnTypes().get(columnBefore));
        iBefore++;
      }
    }
  }
}

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
    for (int i = 0; i < valuesBefore.length; i++) {
      visitor.visit(columnsPresentBefore.get(i), tableMap.getColumnTypes().get(i));
    }
  }
}

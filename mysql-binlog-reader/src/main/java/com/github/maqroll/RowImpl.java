package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.Row;
import com.github.mheath.netty.codec.mysql.RowVisitor;
import java.util.List;

public class RowImpl implements Row {
  private final Object[] values;
  private final List<Integer> columnsPresent;
  private final TableMapEventPayload tableMap;

  public RowImpl(TableMapEventPayload tableMap, List<Integer> columnsPresent, Object[] values) {
    this.tableMap = tableMap;
    this.columnsPresent = columnsPresent;
    this.values = values;
  }

  @Override
  public void accept(RowVisitor visitor) {
    for (int i = 0; i < values.length; i++) {
      visitor.visit(columnsPresent.get(i), tableMap.getColumnTypes().get(i));
    }
  }
}

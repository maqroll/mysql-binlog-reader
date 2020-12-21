package com.github.maqroll;

import com.github.mheath.netty.codec.mysql.ResultsetRow;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ResultSet {
  private static final AttributeKey<ResultSet> key =
      AttributeKey.newInstance(ResultSet.class.getName());

  private List<ResultsetRow> rows = new ArrayList<ResultsetRow>();

  private ResultSet() {}

  public static ResultSet getCurrent(Channel ch) {
    final Attribute<ResultSet> attr = ch.attr(key);
    ResultSet current = attr.get();

    if (current == null) {
      current = attr.setIfAbsent(new ResultSet());
    }

    return current;
  }

  public static void clear(Channel ch) {
    final Attribute<ResultSet> attr = ch.attr(key);
    attr.set(null);
  }

  public void setCurrent(Channel ch) {
    final Attribute<ResultSet> attr = ch.attr(key);
    attr.set(this);
  }

  public void addRow(ResultsetRow row) {
    rows.add(row);
  }

  public List<ResultsetRow> rows() {
    return Collections.unmodifiableList(rows);
  }
}

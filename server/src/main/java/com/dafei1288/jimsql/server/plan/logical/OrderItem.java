package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.common.meta.JqColumn;

/** Simple ordering item: column + direction. */
public class OrderItem {
  private JqColumn column;
  private boolean asc = true; // default ASC

  public OrderItem() {}
  public OrderItem(JqColumn column, boolean asc) {
    this.column = column;
    this.asc = asc;
  }

  public JqColumn getColumn() { return column; }
  public void setColumn(JqColumn column) { this.column = column; }
  public boolean isAsc() { return asc; }
  public void setAsc(boolean asc) { this.asc = asc; }

  @Override
  public String toString() {
    return "OrderItem{" +
        "column=" + column +
        ", asc=" + asc +
        '}';
  }
}

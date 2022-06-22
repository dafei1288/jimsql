package com.dafei1288.jimsql.common.meta;

public class JqColumn {
  private String columnName;
  private Class columnClazzType;
  private JqTable table;
  private int columnType;
  private int size;

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public Class getColumnClazzType() {
    return columnClazzType;
  }

  public void setColumnClazzType(Class columnClazzType) {
    this.columnClazzType = columnClazzType;
  }

  public JqTable getTable() {
    return table;
  }

  public void setTable(JqTable table) {
    this.table = table;
  }

  public int getColumnType() {
    return columnType;
  }

  public void setColumnType(int columnType) {
    this.columnType = columnType;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  @Override
  public String toString() {
    return "JqColumn{" +
        "columnName='" + columnName + '\'' +
        ", columnClazzType=" + columnClazzType +
        ", table=" + table +
        ", columnType=" + columnType +
        ", size=" + size +
        '}';
  }
}

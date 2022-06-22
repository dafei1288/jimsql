package com.dafei1288.jimsql.common;

import java.io.Serializable;

public class JqColumnResultSetMetadata implements Serializable {
  private String labelName;
  private Integer index;

  private Integer columnType;
  private Class Clazz;
  private String clazzStr;

  private String tableName;

  public String getLabelName() {
    return labelName;
  }

  public void setLabelName(String labelName) {
    this.labelName = labelName;
  }

  public Integer getIndex() {
    return index;
  }

  public void setIndex(Integer index) {
    this.index = index;
  }

  public Class getClazz() {
    return Clazz;
  }

  public void setClazz(Class clazz) {
    Clazz = clazz;
  }

  public String getClazzStr() {
    return clazzStr;
  }

  public void setClazzStr(String clazzStr) {
    this.clazzStr = clazzStr;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Integer getColumnType() {
    return columnType;
  }

  public void setColumnType(Integer columnType) {
    this.columnType = columnType;
  }

  @Override
  public String toString() {
    return "JqColumnMetadata{" +
        "labelName='" + labelName + '\'' +
        ", index=" + index +
        ", columnType=" + columnType +
        ", Clazz=" + Clazz +
        ", clazzStr='" + clazzStr + '\'' +
        ", tableName='" + tableName + '\'' +
        '}';
  }
}

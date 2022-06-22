package com.dafei1288.jimsql.common.meta;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;

public class JqTable {
  private File basepath;
  private String tableName;
  private JqDatabase jqDatabase;
  private LinkedHashMap<String,JqColumn> jqTableLinkedHashMap;

  public File getBasepath() {
    return basepath;
  }

  public void setBasepath(File basepath) {
    this.basepath = basepath;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public JqDatabase getJqDatabase() {
    return jqDatabase;
  }

  public void setJqDatabase(JqDatabase jqDatabase) {
    this.jqDatabase = jqDatabase;
  }

  public LinkedHashMap<String, JqColumn> getJqTableLinkedHashMap() {
    return jqTableLinkedHashMap;
  }

  public void setJqTableLinkedHashMap(
      LinkedHashMap<String, JqColumn> jqTableLinkedHashMap) {
    this.jqTableLinkedHashMap = jqTableLinkedHashMap;
  }

  @Override
  public String toString() {
    return "JqTable{" +
        "basepath=" + basepath +
        ", tableName='" + tableName + '\'' +
        ", jqDatabase=" + jqDatabase +
        ", jqTableLinkedHashMap=" + jqTableLinkedHashMap +
        '}';
  }
}

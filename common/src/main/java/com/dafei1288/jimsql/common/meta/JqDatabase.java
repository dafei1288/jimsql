package com.dafei1288.jimsql.common.meta;

import java.io.File;
import java.util.LinkedHashMap;

public class JqDatabase {
  private File basePath;
  private String databaseName;
  private LinkedHashMap<String,JqTable> jqTableListMap;

  public File getBasePath() {
    return basePath;
  }

  public void setBasePath(File basePath) {
    this.basePath = basePath;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public LinkedHashMap<String, JqTable> getJqTableListMap() {
    return jqTableListMap;
  }

  public void setJqTableListMap(
      LinkedHashMap<String, JqTable> jqTableListMap) {
    this.jqTableListMap = jqTableListMap;
  }

  @Override
  public String toString() {
    return "JqDatabase{" +
        "basePath=" + basePath +
        ", databaseName='" + databaseName + '\'' +
        ", jqTableListMap=" + jqTableListMap +
        '}';
  }
}

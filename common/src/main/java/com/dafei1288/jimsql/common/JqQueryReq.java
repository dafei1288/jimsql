package com.dafei1288.jimsql.common;

import java.io.Serializable;

public class JqQueryReq implements Serializable {
  private String db;
  private String sql;

  public String getDb() {
    return db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  @Override
  public String toString() {
    return "JqQueryReq{" +
        "db='" + db + '\'' +
        ", sql='" + sql + '\'' +
        '}';
  }
}

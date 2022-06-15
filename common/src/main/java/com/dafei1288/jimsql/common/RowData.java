package com.dafei1288.jimsql.common;

import java.io.Serializable;
import java.util.Map;

public class RowData implements Serializable {
  private Map<String,Object> datas;
  private boolean next;

  public Map<String, Object> getDatas() {
    return datas;
  }

  public void setDatas(Map<String, Object> datas) {
    this.datas = datas;
  }

  public boolean isNext() {
    return next;
  }

  public void setNext(boolean next) {
    this.next = next;
  }

  @Override
  public String toString() {
    return "RowData{" +
        "datas=" + datas +
        ", next=" + next +
        '}';
  }
}

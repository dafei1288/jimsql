package com.dafei1288.jimsql.server.plan.logical;

/**
 * One SELECT item: a column (possibly qualified) with optional alias.
 * Example: "u.id AS uid" -> columnName="u.id", alias="uid".
 */
public class SelectItem {
  private String columnName; // parsed column or expression label (qualified or simple)
  private String alias;      // optional; when present becomes output label

  public String getColumnName() { return columnName; }
  public void setColumnName(String columnName) { this.columnName = columnName; }

  public String getAlias() { return alias; }
  public void setAlias(String alias) { this.alias = alias; }

  @Override
  public String toString() {
    return "SelectItem{" +
        "columnName='" + columnName + '\'' +
        ", alias='" + alias + '\'' +
        '}';
  }
}

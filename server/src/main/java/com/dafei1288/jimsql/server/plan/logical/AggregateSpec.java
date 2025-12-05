package com.dafei1288.jimsql.server.plan.logical;

// Simple aggregate spec: type + argument column + optional alias
public class AggregateSpec {
  public enum Type { COUNT, SUM, AVG, MIN, MAX }
  private Type type;
  private String column; // null for COUNT(*) / COUNT(1)
  private String alias;  // optional

  public AggregateSpec() {}
  public AggregateSpec(Type type, String column, String alias) {
    this.type = type; this.column = column; this.alias = alias;
  }
  public Type getType() { return type; }
  public void setType(Type type) { this.type = type; }
  public String getColumn() { return column; }
  public void setColumn(String column) { this.column = column; }
  public String getAlias() { return alias; }
  public void setAlias(String alias) { this.alias = alias; }
}
package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.common.meta.JqTable;

/** Join description for logical plan (no execution semantics). */
public class JoinSpec {
  private JoinType type = JoinType.INNER;
  private JqTable rightTable; // joined table (rhs)
  private String onExpression; // raw condition text
  private String alias; // optional alias for right table

  public JoinType getType() { return type; }
  public void setType(JoinType type) { this.type = type; }

  public JqTable getRightTable() { return rightTable; }
  public void setRightTable(JqTable rightTable) { this.rightTable = rightTable; }

  public String getOnExpression() { return onExpression; }
  public void setOnExpression(String onExpression) { this.onExpression = onExpression; }

  public String getAlias() { return alias; }
  public void setAlias(String alias) { this.alias = alias; }

  @Override
  public String toString() {
    return "JoinSpec{" +
        "type=" + type +
        ", rightTable=" + rightTable +
        ", onExpression='" + onExpression + '\'' +
        ", alias='" + alias + '\'' +
        '}';
  }
}

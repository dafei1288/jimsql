package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;

// Minimal logical plan for DELETE; execution handled by CSV executor or protocol handler
public class DeleteLogicalPlan implements LogicalPlan {
  private JqTable table;
  private String whereExpression; // nullable
  private JqDatabase currentDatabase; // optional

  public JqTable getTable() { return table; }
  public void setTable(JqTable table) { this.table = table; }

  public String getWhereExpression() { return whereExpression; }
  public void setWhereExpression(String whereExpression) { this.whereExpression = whereExpression; }

  public JqDatabase getCurrentDatabase() { return currentDatabase; }
  public void setCurrentDatabase(JqDatabase currentDatabase) { this.currentDatabase = currentDatabase; }

  @Override
  public PhysicalPlan transform() { return null; }
}

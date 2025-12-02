package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;
import java.util.LinkedHashMap;

// Minimal logical plan for UPDATE; execution handled by CSV executor or protocol handler
public class UpdateLogicalPlan implements LogicalPlan {
  private JqTable table;
  private LinkedHashMap<String,String> assignments = new LinkedHashMap<>();
  private String whereExpression; // nullable
  private JqDatabase currentDatabase; // optional

  public JqTable getTable() { return table; }
  public void setTable(JqTable table) { this.table = table; }

  public LinkedHashMap<String,String> getAssignments() { return assignments; }
  public void setAssignments(LinkedHashMap<String,String> assignments) { this.assignments = assignments; }

  public String getWhereExpression() { return whereExpression; }
  public void setWhereExpression(String whereExpression) { this.whereExpression = whereExpression; }

  public JqDatabase getCurrentDatabase() { return currentDatabase; }
  public void setCurrentDatabase(JqDatabase currentDatabase) { this.currentDatabase = currentDatabase; }

  @Override
  public PhysicalPlan transform() { return null; }
}

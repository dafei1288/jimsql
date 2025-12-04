package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;
import java.util.ArrayList;
import java.util.List;

// Minimal logical plan for INSERT ... VALUES
public class InsertLogicalPlan implements LogicalPlan {
  private JqTable table;
  private List<String> columns = new ArrayList<>(); // optional; when empty, use table header order
  private List<List<String>> rows = new ArrayList<>(); // each row is a list of string literals (unquoted)
  private JqDatabase currentDatabase;

  public JqTable getTable() { return table; }
  public void setTable(JqTable table) { this.table = table; }

  public List<String> getColumns() { return columns; }
  public void setColumns(List<String> columns) { this.columns = columns; }

  public List<List<String>> getRows() { return rows; }
  public void setRows(List<List<String>> rows) { this.rows = rows; }

  public JqDatabase getCurrentDatabase() { return currentDatabase; }
  public void setCurrentDatabase(JqDatabase currentDatabase) { this.currentDatabase = currentDatabase; }

  @Override
  public PhysicalPlan transform() { return null; }
}
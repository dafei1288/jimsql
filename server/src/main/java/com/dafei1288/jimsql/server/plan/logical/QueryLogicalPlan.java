package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;
import com.dafei1288.jimsql.server.plan.physical.QueryPhysicalPlan;
import java.util.List;

public class QueryLogicalPlan implements LogicalPlan{
  private boolean star;
  private List<JqColumn> jqColumnList;
  private JqTable fromTable;



  private OptimizeQueryLogicalPlan optimizeQueryLogicalPlan;


  public boolean isStar() {
    return star;
  }

  public void setStar(boolean star) {
    this.star = star;
  }

  public List<JqColumn> getJqColumnList() {
    return jqColumnList;
  }

  public void setJqColumnList(List<JqColumn> jqColumnList) {
    this.jqColumnList = jqColumnList;
  }

  public JqTable getFromTable() {
    return fromTable;
  }

  public void setFromTable(JqTable fromTable) {
    this.fromTable = fromTable;
  }


  @Override
  public String toString() {
    return "QueryLogicalPlan{" +
        "star=" + star +
        ", jqColumnList=" + jqColumnList +
        ", fromTable=" + fromTable +
        ", optimizeQueryLogicalPlan=" + optimizeQueryLogicalPlan +
        '}';
  }

  public OptimizeQueryLogicalPlan optimizeQueryLogicalPlan(JqDatabase jqDatabase){
    this.optimizeQueryLogicalPlan = new OptimizeQueryLogicalPlan(this);
    optimizeQueryLogicalPlan.setCurrentDatabase(jqDatabase);
    optimizeQueryLogicalPlan.optimize();
    return this.optimizeQueryLogicalPlan;
  }

  public OptimizeQueryLogicalPlan optimizeQueryLogicalPlan(){
    return  optimizeQueryLogicalPlan(this.getFromTable().getJqDatabase());
  }

  public  PhysicalPlan transform(OptimizeQueryLogicalPlan queryLogicalPlan){
    PhysicalPlan physicalPlan = new QueryPhysicalPlan();
    if(queryLogicalPlan == null){
      queryLogicalPlan = this.optimizeQueryLogicalPlan();
    }
    physicalPlan.setLogicalPlan(queryLogicalPlan);
    return physicalPlan;
  }

  @Override
  public PhysicalPlan transform() {
    return this.transform(this.optimizeQueryLogicalPlan);
  }
}

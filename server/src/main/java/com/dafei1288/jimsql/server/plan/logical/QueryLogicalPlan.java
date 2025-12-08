package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;
import com.dafei1288.jimsql.server.plan.physical.QueryPhysicalPlan;
import java.util.List;
import java.util.ArrayList;

public class QueryLogicalPlan implements LogicalPlan {
  private boolean star;
  private List<JqColumn> jqColumnList;
  private JqTable fromTable;

  // Optional SELECT clauses (not used by executor yet)
  private List<OrderItem> orderBy = new ArrayList<>();
  private Integer limit;   // null means no limit
  private Integer offset;  // null means no offset
  private List<JqColumn> groupByColumns = new ArrayList<>();
  private String havingExpression; // raw text
  private String whereExpression;  // raw text
  private List<JoinSpec> joins = new ArrayList<>();
private boolean countStar;
private LlmFunctionSpec llmFunctionSpec;
private OptimizeQueryLogicalPlan optimizeQueryLogicalPlan;
  private List<AggregateSpec> aggregates = new ArrayList<>();

  public boolean isStar() { return star; }
  public void setStar(boolean star) { this.star = star; }

  public List<JqColumn> getJqColumnList() { return jqColumnList; }
  public void setJqColumnList(List<JqColumn> jqColumnList) { this.jqColumnList = jqColumnList; }

  public JqTable getFromTable() { return fromTable; }
  public void setFromTable(JqTable fromTable) { this.fromTable = fromTable; }

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
    return optimizeQueryLogicalPlan(this.getFromTable().getJqDatabase());
  }

  public PhysicalPlan transform(OptimizeQueryLogicalPlan queryLogicalPlan){
    PhysicalPlan physicalPlan = new QueryPhysicalPlan();
    if (queryLogicalPlan == null) {
      queryLogicalPlan = this.optimizeQueryLogicalPlan();
    }
    physicalPlan.setLogicalPlan(queryLogicalPlan);
    return physicalPlan;
  }

  @Override
  public PhysicalPlan transform() { return this.transform(this.optimizeQueryLogicalPlan); }

  public List<OrderItem> getOrderBy() { return orderBy; }
  public void setOrderBy(List<OrderItem> orderBy) { this.orderBy = orderBy; }
  public Integer getLimit() { return limit; }
  public void setLimit(Integer limit) { this.limit = limit; }
  public Integer getOffset() { return offset; }
  public void setOffset(Integer offset) { this.offset = offset; }
  public List<JqColumn> getGroupByColumns() { return groupByColumns; }
  public void setGroupByColumns(List<JqColumn> groupByColumns) { this.groupByColumns = groupByColumns; }
  public String getHavingExpression() { return havingExpression; }
  public void setHavingExpression(String havingExpression) { this.havingExpression = havingExpression; }
  public String getWhereExpression() { return whereExpression; }
  public void setWhereExpression(String whereExpression) { this.whereExpression = whereExpression; }
  public List<JoinSpec> getJoins() { return joins; }
  public void setJoins(List<JoinSpec> joins) { this.joins = joins; }
  public boolean isCountStar() { return countStar; }
  public void setCountStar(boolean countStar) { this.countStar = countStar; }
  public List<AggregateSpec> getAggregates() { return aggregates; }
  public void setAggregates(List<AggregateSpec> aggregates) { this.aggregates = aggregates; }
  public LlmFunctionSpec getLlmFunctionSpec() { return llmFunctionSpec; }
  public void setLlmFunctionSpec(LlmFunctionSpec llmFunctionSpec) { this.llmFunctionSpec = llmFunctionSpec; }
}
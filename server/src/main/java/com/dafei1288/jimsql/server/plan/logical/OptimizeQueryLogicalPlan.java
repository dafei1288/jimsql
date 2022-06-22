package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.common.JqColumnResultSetMetadata;
import com.dafei1288.jimsql.common.JqResultSetMetaData;
import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.server.instance.ServerMetadata;
import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;
import com.dafei1288.jimsql.server.plan.physical.QueryPhysicalPlan;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OptimizeQueryLogicalPlan implements LogicalPlan {

  private JqDatabase currentDatabase;

  private QueryLogicalPlan queryLogicalPlan;
  private JqResultSetMetaData jqResultSetMetaData;
  private LinkedHashMap<String,JqColumnResultSetMetadata> jqColumnResultSetMetadataList;

  public JqResultSetMetaData getJqResultSetMetaData() {
    return jqResultSetMetaData;
  }

  public void setJqResultSetMetaData(JqResultSetMetaData jqResultSetMetaData) {
    this.jqResultSetMetaData = jqResultSetMetaData;
  }

  public LinkedHashMap<String, JqColumnResultSetMetadata> getJqColumnResultSetMetadataList() {
    return jqColumnResultSetMetadataList;
  }

  public void setJqColumnResultSetMetadataList(
      LinkedHashMap<String, JqColumnResultSetMetadata> jqColumnResultSetMetadataList) {
    this.jqColumnResultSetMetadataList = jqColumnResultSetMetadataList;
  }

  public OptimizeQueryLogicalPlan(QueryLogicalPlan queryLogicalPlan){
    this.setQueryLogicalPlan(queryLogicalPlan);
  }

  public QueryLogicalPlan getQueryLogicalPlan() {
    return queryLogicalPlan;
  }

  public JqDatabase getCurrentDatabase() {
    return currentDatabase;
  }

  public void setCurrentDatabase(JqDatabase currentDatabase) {
    this.currentDatabase = currentDatabase;
  }

  public void setQueryLogicalPlan(
      QueryLogicalPlan queryLogicalPlan) {
    this.queryLogicalPlan = queryLogicalPlan;
  }

  @Override
  public PhysicalPlan transform() {
    PhysicalPlan physicalPlan = new QueryPhysicalPlan();
    physicalPlan.setLogicalPlan(this);
    return physicalPlan;
  }

  public void optimize() {
    this.prepareMetadata();
  }

  private void prepareMetadata(){
    this.jqColumnResultSetMetadataList = new LinkedHashMap<>();
    this.jqResultSetMetaData = new JqResultSetMetaData(this.jqColumnResultSetMetadataList);

    ServerMetadata serverMetadata = ServerMetadata.getInstance();
    String currentDatabaseName = this.getCurrentDatabase().getDatabaseName();
    String currentTableName = this.queryLogicalPlan.getFromTable().getTableName();
    List<String> colNames = new ArrayList<>();

    if(this.queryLogicalPlan.isStar()){
      Set<String> cols = serverMetadata.fetchTableByName(currentDatabaseName,currentTableName).getJqTableLinkedHashMap().keySet();
      colNames = cols.stream().toList();
    }else{
      colNames = this.queryLogicalPlan.getJqColumnList().stream().map(it->it.getColumnName()).collect(Collectors.toList());
    }

    List<JqColumn> jqColumnList = serverMetadata.fetchColumnsByName(currentDatabaseName,currentTableName,colNames);
    for(int i = 0; i < jqColumnList.size(); i++ ){
      JqColumn jqColumn = jqColumnList.get(i);

      JqColumnResultSetMetadata jqColumnMetadata = new JqColumnResultSetMetadata();
      jqColumnMetadata.setIndex(i+1);
      jqColumnMetadata.setLabelName(jqColumn.getColumnName());
      jqColumnMetadata.setClazz(jqColumn.getColumnClazzType());
      jqColumnMetadata.setClazzStr(jqColumn.getColumnClazzType().getName());
      jqColumnMetadata.setTableName(jqColumn.getTable().getTableName());
      jqColumnMetadata.setColumnType(jqColumn.getColumnType());

      this.jqColumnResultSetMetadataList.put(jqColumnMetadata.getLabelName(),jqColumnMetadata);
    }



    this.jqResultSetMetaData.setColumnMeta(this.jqColumnResultSetMetadataList);
  }

}

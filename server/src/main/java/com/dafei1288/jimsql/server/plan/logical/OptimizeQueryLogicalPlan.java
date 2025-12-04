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

    // If SELECT COUNT(*) (optionally with GROUP BY), synthesize metadata accordingly
    if (this.queryLogicalPlan.isCountStar()) {
      java.util.LinkedHashMap<String, JqColumnResultSetMetadata> out = new java.util.LinkedHashMap<>();
      int idx = 1;
      java.util.List<com.dafei1288.jimsql.common.meta.JqColumn> gcols = this.queryLogicalPlan.getGroupByColumns();
      if (gcols != null && !gcols.isEmpty()) {
        for (com.dafei1288.jimsql.common.meta.JqColumn gc : gcols) {
          JqColumnResultSetMetadata m = new JqColumnResultSetMetadata();
          m.setIndex(idx++);
          m.setLabelName(gc.getColumnName());
          m.setClazz(String.class);
          m.setClazzStr("java.lang.String");
          m.setTableName(this.queryLogicalPlan.getFromTable().getTableName());
          m.setColumnType(java.sql.Types.VARCHAR);
          out.put(m.getLabelName(), m);
        }
      }
      JqColumnResultSetMetadata cnt = new JqColumnResultSetMetadata();
      cnt.setIndex(idx);
      cnt.setLabelName("count");
      cnt.setClazz(Long.class);
      cnt.setClazzStr("java.lang.Long");
      cnt.setTableName("");
      cnt.setColumnType(java.sql.Types.BIGINT);
      out.put("count", cnt);
      this.jqColumnResultSetMetadataList.clear();
      this.jqColumnResultSetMetadataList.putAll(out);
      this.jqResultSetMetaData.setColumnMeta(this.jqColumnResultSetMetadataList);
      return;
    }

    Set<String> cols = serverMetadata.fetchTableByName(currentDatabaseName,currentTableName).getJqTableLinkedHashMap().keySet();
    colNames = cols.stream().toList();
    java.util.List<JqColumn> jqColumnList = serverMetadata.fetchColumnsByName(currentDatabaseName,currentTableName,colNames);
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



    // Fallback: if selection is empty, default to all columns
    if (this.jqColumnResultSetMetadataList.isEmpty()) {
      com.dafei1288.jimsql.common.meta.JqTable _jt = ServerMetadata.getInstance().fetchTableByName(currentDatabaseName,currentTableName);
      java.util.List<com.dafei1288.jimsql.common.meta.JqColumn> _all = new java.util.ArrayList<>(_jt.getJqTableLinkedHashMap().values());
      for (int i = 0; i < _all.size(); i++) {
        com.dafei1288.jimsql.common.meta.JqColumn jqColumn = _all.get(i);
        JqColumnResultSetMetadata jqColumnMetadata = new JqColumnResultSetMetadata();
        jqColumnMetadata.setIndex(i+1);
        jqColumnMetadata.setLabelName(jqColumn.getColumnName());
        jqColumnMetadata.setClazz(jqColumn.getColumnClazzType());
        jqColumnMetadata.setClazzStr(jqColumn.getColumnClazzType().getName());
        jqColumnMetadata.setTableName(jqColumn.getTable().getTableName());
        jqColumnMetadata.setColumnType(jqColumn.getColumnType());
        this.jqColumnResultSetMetadataList.put(jqColumnMetadata.getLabelName(),jqColumnMetadata);
      }
    }
    this.jqResultSetMetaData.setColumnMeta(this.jqColumnResultSetMetadataList);
  }

}

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
    // Aggregates (SUM/AVG/MIN/MAX) with optional GROUP BY: build result metadata
    if (this.queryLogicalPlan.getAggregates() != null && !this.queryLogicalPlan.getAggregates().isEmpty()) {
      java.util.LinkedHashMap<String, JqColumnResultSetMetadata> out = new java.util.LinkedHashMap<>();
      com.dafei1288.jimsql.common.meta.JqTable jt = serverMetadata.fetchTableByName(currentDatabaseName, currentTableName);
      int idx = 1;
      java.util.List<com.dafei1288.jimsql.common.meta.JqColumn> gcols = this.queryLogicalPlan.getGroupByColumns();
      if (gcols != null && !gcols.isEmpty()) {
        for (com.dafei1288.jimsql.common.meta.JqColumn gc : gcols) {
          com.dafei1288.jimsql.common.meta.JqColumn src = findColumnIgnoreCase(jt, gc.getColumnName());
          JqColumnResultSetMetadata m = new JqColumnResultSetMetadata();
          m.setIndex(idx++);
          m.setLabelName(gc.getColumnName());
          if (src != null) {
            m.setClazz(src.getColumnClazzType());
            m.setClazzStr(src.getColumnClazzType().getName());
            m.setColumnType(src.getColumnType());
          } else {
            m.setClazz(String.class);
            m.setClazzStr("java.lang.String");
            m.setColumnType(java.sql.Types.VARCHAR);
          }
          m.setTableName(currentTableName);
          out.put(m.getLabelName(), m);
        }
      }
      for (AggregateSpec spec : this.queryLogicalPlan.getAggregates()) {
        String srcCol = spec.getColumn();
        String label;
        if (spec.getAlias() != null && !spec.getAlias().isEmpty()) label = spec.getAlias();
        else {
          switch (spec.getType()) {
            case COUNT: label = (srcCol == null || srcCol.equals("*")) ? "count" : ("count_" + normalizeCol(srcCol)); break;
            case SUM:   label = "sum_" + normalizeCol(srcCol); break;
            case AVG:   label = "avg_" + normalizeCol(srcCol); break;
            case MIN:   label = "min_" + normalizeCol(srcCol); break;
            case MAX:   label = "max_" + normalizeCol(srcCol); break;
            default:    label = "agg"; break;
          }
        }
        JqColumnResultSetMetadata m = new JqColumnResultSetMetadata();
        m.setIndex(idx++);
        m.setLabelName(label);
        switch (spec.getType()) {
          case COUNT:
            m.setClazz(Long.class); m.setClazzStr("java.lang.Long"); m.setColumnType(java.sql.Types.BIGINT); m.setTableName("");
            break;
          case SUM:
          case AVG:
            m.setClazz(java.math.BigDecimal.class); m.setClazzStr("java.math.BigDecimal"); m.setColumnType(java.sql.Types.DECIMAL); m.setTableName("");
            break;
          case MIN:
          case MAX:
            com.dafei1288.jimsql.common.meta.JqColumn src = srcCol == null ? null : findColumnIgnoreCase(jt, srcCol);
            if (src != null) {
              m.setClazz(src.getColumnClazzType()); m.setClazzStr(src.getColumnClazzType().getName()); m.setColumnType(src.getColumnType());
            } else {
              m.setClazz(String.class); m.setClazzStr("java.lang.String"); m.setColumnType(java.sql.Types.VARCHAR);
            }
            m.setTableName("");
            break;
        }
        out.put(m.getLabelName(), m);
      }
      this.jqColumnResultSetMetadataList.clear();
      this.jqColumnResultSetMetadataList.putAll(out);
      this.jqResultSetMetaData.setColumnMeta(this.jqColumnResultSetMetadataList);
      return;
    }

    // JOIN + SELECT *: build metadata from left table plus each right table with alias prefix
    if (this.queryLogicalPlan.isStar() && this.queryLogicalPlan.getJoins() != null && !this.queryLogicalPlan.getJoins().isEmpty()) {
      java.util.LinkedHashMap<String, JqColumnResultSetMetadata> out = new java.util.LinkedHashMap<>();
      int idx = 1;
      com.dafei1288.jimsql.common.meta.JqTable lt = serverMetadata.fetchTableByName(currentDatabaseName, currentTableName);
      for (com.dafei1288.jimsql.common.meta.JqColumn c : lt.getJqTableLinkedHashMap().values()) {
        JqColumnResultSetMetadata m = new JqColumnResultSetMetadata();
        m.setIndex(idx++);
        m.setLabelName(c.getColumnName());
        m.setClazz(c.getColumnClazzType());
        m.setClazzStr(c.getColumnClazzType().getName());
        m.setTableName(lt.getTableName());
        m.setColumnType(c.getColumnType());
        out.put(m.getLabelName(), m);
      }
      for (com.dafei1288.jimsql.server.plan.logical.JoinSpec js : this.queryLogicalPlan.getJoins()) {
        com.dafei1288.jimsql.common.meta.JqTable rt = serverMetadata.fetchTableByName(currentDatabaseName, js.getRightTable().getTableName());
        String rPrefix = (js.getAlias()!=null && !js.getAlias().isEmpty()) ? js.getAlias() : rt.getTableName();
        for (com.dafei1288.jimsql.common.meta.JqColumn c : rt.getJqTableLinkedHashMap().values()) {
          JqColumnResultSetMetadata m = new JqColumnResultSetMetadata();
          m.setIndex(idx++);
          m.setLabelName(rPrefix + "." + c.getColumnName());
          m.setClazz(c.getColumnClazzType());
          m.setClazzStr(c.getColumnClazzType().getName());
          m.setTableName(rt.getTableName());
          m.setColumnType(c.getColumnType());
          out.put(m.getLabelName(), m);
        }
      }
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
  
  private static String normalizeCol(String c){ if (c==null) return ""; int d=c.lastIndexOf('.'); if (d>=0) c=c.substring(d+1); if (c.startsWith("`")&&c.endsWith("`")) c=c.substring(1,c.length()-1); if (c.startsWith("\"")&&c.endsWith("\"")) c=c.substring(1,c.length()-1); return c; }
  private static com.dafei1288.jimsql.common.meta.JqColumn findColumnIgnoreCase(com.dafei1288.jimsql.common.meta.JqTable jt, String name){ if (jt==null||name==null) return null; String n = normalizeCol(name); for (String k : jt.getJqTableLinkedHashMap().keySet()){ if (k.equalsIgnoreCase(n)) return jt.getJqTableLinkedHashMap().get(k); } return null; }}

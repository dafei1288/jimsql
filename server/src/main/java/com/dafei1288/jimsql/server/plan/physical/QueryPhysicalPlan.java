package com.dafei1288.jimsql.server.plan.physical;

import com.dafei1288.jimsql.common.JqColumnResultSetMetadata;
import com.dafei1288.jimsql.common.RowData;
import com.dafei1288.jimsql.common.Utils;
import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.instance.ServerMetadata;
import com.dafei1288.jimsql.server.plan.logical.LogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.OptimizeQueryLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.OrderItem;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import com.google.common.io.Files;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryPhysicalPlan implements PhysicalPlan{
  private LogicalPlan logicalPlan;

  @Override
  public void setLogicalPlan(LogicalPlan logicalPlan) {
    this.logicalPlan = logicalPlan;
  }

  @Override
  public LogicalPlan getLogicalPlan() {
    return logicalPlan;
  }

  @Override
  public void proxyWrite(ChannelHandlerContext ctx) throws IOException {
    OptimizeQueryLogicalPlan optimizeQueryLogicalPlan = (OptimizeQueryLogicalPlan)logicalPlan;
    QueryLogicalPlan qlp = optimizeQueryLogicalPlan.getQueryLogicalPlan();
    String currentDatabase = optimizeQueryLogicalPlan.getCurrentDatabase().getDatabaseName();
    String currentTable = qlp.getFromTable().getTableName();

    ServerMetadata sm = ServerMetadata.getInstance();
    JqDatabase jqDatabase = sm.fetchDatabaseByName(currentDatabase);
    JqTable jqTable = sm.fetchTableByName(currentDatabase,currentTable);

        // Read left file lines
    List<String> datas = Files.readLines(jqTable.getBasepath(), Charset.defaultCharset());
    if (datas.isEmpty()) return;

    // Header -> left column order
    List<String> header = new ArrayList<>(jqTable.getJqTableLinkedHashMap().keySet());

    // Load left rows
    List<Map<String,String>> leftRows = new ArrayList<>();
    for (int i = 1; i < datas.size(); i++) {
      String data = datas.get(i);
      String[] rowdataStr = data.split(Utils.COLUMN_SPILTOR, -1);
      LinkedHashMap<String,String> full = new LinkedHashMap<>();
      int j = 0;
      for (String key : header) {
        String v = (j < rowdataStr.length) ? rowdataStr[j] : "";
        full.put(key, v);
        j++;
      }
      leftRows.add(full);
    }

    // Apply JOINs (INNER/LEFT/CROSS) before WHERE if any
    List<Map<String,String>> fullRows = leftRows;
    if (qlp.getJoins() != null && !qlp.getJoins().isEmpty()) {
      for (com.dafei1288.jimsql.server.plan.logical.JoinSpec js : qlp.getJoins()) {
        // load right table
        com.dafei1288.jimsql.common.meta.JqTable rt = sm.fetchTableByName(currentDatabase, js.getRightTable().getTableName());
        List<String> rdata = Files.readLines(rt.getBasepath(), Charset.defaultCharset());
        if (rdata.size() <= 1) { if (js.getType()==com.dafei1288.jimsql.server.plan.logical.JoinType.INNER) { fullRows = new ArrayList<>(); } continue; }
        List<String> rheader = new ArrayList<>(rt.getJqTableLinkedHashMap().keySet());
        List<Map<String,String>> rightRows = new ArrayList<>();
        for (int i = 1; i < rdata.size(); i++) {
          String line = rdata.get(i);
          String[] parts = line.split(Utils.COLUMN_SPILTOR, -1);
          LinkedHashMap<String,String> row = new LinkedHashMap<>();
          int k = 0; for (String key : rheader) { row.put(key, (k < parts.length) ? parts[k] : ""); k++; }
          rightRows.add(row);
        }
        // parse ON: support simple AND of equality: <id> = <id>
        java.util.List<String[]> eqs = parseJoinOnEquals(js.getOnExpression());
        String rAlias = (js.getAlias()!=null && !js.getAlias().isEmpty()) ? js.getAlias() : rt.getTableName();
        // DEBUG: print JOIN ON and headers
        try {
          String eqsDbg = (eqs==null?"": eqs.stream().map(a -> a[0]+"="+a[1]).collect(java.util.stream.Collectors.joining(" AND ")));
          System.out.println("JOIN DEBUG on=[" + js.getOnExpression() + "] rAlias=" + rAlias + " eqs=" + eqsDbg + " leftHeader=" + header + " rightHeader=" + rheader);
        } catch (Throwable ignore) {}
        if (eqs == null || eqs.isEmpty()) {
          // No valid ON equality parsed: avoid accidental Cartesian product
          if (js.getType()==com.dafei1288.jimsql.server.plan.logical.JoinType.INNER) {
            fullRows = new java.util.ArrayList<>();
            break;
          } else if (js.getType()==com.dafei1288.jimsql.server.plan.logical.JoinType.LEFT) {
            java.util.List<java.util.Map<String,String>> newRows = new java.util.ArrayList<>();
            for (java.util.Map<String,String> lr : fullRows) {
              java.util.LinkedHashMap<String,String> out = new java.util.LinkedHashMap<>();
              for (String k : header) out.put(k, lr.get(k));
              for (String rk2 : rheader) out.put(rAlias+"."+rk2, null);
              newRows.add(out);
            }
            java.util.List<String> newHeader = new java.util.ArrayList<>(header);
            for (String rk2 : rheader) newHeader.add(rAlias+"."+rk2);
            header = newHeader;
            fullRows = newRows;
            continue;
          }
        }
        // build RHS hash by key tuple
        java.util.Map<String, java.util.List<Map<String,String>>> rhs = new java.util.HashMap<>();
        for (Map<String,String> rr : rightRows) {
          String rk = buildJoinKey(rr, header, rheader, rAlias, false /* isLeft */, eqs);
          rhs.computeIfAbsent(rk, t -> new java.util.ArrayList<>()).add(rr);
          // DEBUG: sample a few right keys
          if (rhs.size() <= 3) {
            try { System.out.println("JOIN DEBUG RKEY=" + rk + " row=" + rr); } catch (Throwable ignore) {}
          }
        }
        // combine
        List<Map<String,String>> newRows = new ArrayList<>();
        for (Map<String,String> lr : fullRows) {
          String lk = buildJoinKey(lr, header, rheader, rAlias, true /* isLeft */, eqs);
          List<Map<String,String>> matches = rhs.get(lk);
          try { System.out.println("JOIN DEBUG LKEY=" + lk + " matches=" + (matches==null?0:matches.size()) + " row=" + lr); } catch (Throwable ignore) {}
          if (matches != null && !matches.isEmpty()) {
            for (Map<String,String> rr : matches) {
              LinkedHashMap<String,String> out = new LinkedHashMap<>();
              // left columns as-is
              for (String k : header) { out.put(k, lr.get(k)); }
              // right columns with prefix alias/table
              String rPrefix = (js.getAlias()!=null && !js.getAlias().isEmpty()) ? js.getAlias() : rt.getTableName();
              for (String rk2 : rheader) { out.put(rPrefix+"."+rk2, rr.get(rk2)); }
              newRows.add(out);
            }
          } else if (js.getType() == com.dafei1288.jimsql.server.plan.logical.JoinType.LEFT) {
            LinkedHashMap<String,String> out = new LinkedHashMap<>();
            for (String k : header) { out.put(k, lr.get(k)); }
            String rPrefix = (js.getAlias()!=null && !js.getAlias().isEmpty()) ? js.getAlias() : rt.getTableName();
            for (String rk2 : rheader) { out.put(rPrefix+"."+rk2, null); }
            newRows.add(out);
          }
        }
        // update header to include right side qualified labels
        String rPrefix = (js.getAlias()!=null && !js.getAlias().isEmpty()) ? js.getAlias() : rt.getTableName();
        List<String> newHeader = new ArrayList<>(header);
        for (String rk2 : rheader) { newHeader.add(rPrefix+"."+rk2); }
        header = newHeader;
        fullRows = newRows;
        if (fullRows.isEmpty() && js.getType()==com.dafei1288.jimsql.server.plan.logical.JoinType.INNER) break;
      }
    }// JOIN pipeline ready

      // WHERE filter (enhanced: AND/OR, parentheses, IS NULL, LIKE, IN)
      String where = qlp.getWhereExpression();
      if (where != null && !where.trim().isEmpty()) {
          WhereEvaluator.Node expr = WhereEvaluator.parse(where);
          final WhereEvaluator.Node ex = expr;
          final JqTable _jt = jqTable;
          // Always evaluate; missing columns will read as "" and evaluate accordingly
          fullRows = fullRows.stream()
                  .filter(r -> {
                      try { return ex.eval(r, _jt); } catch (Throwable t) { return true; }
                  })
                  .collect(java.util.stream.Collectors.toList());
      }


        // Aggregation: general aggregates (SUM/AVG/MIN/MAX/COUNT) with optional GROUP BY
    if (qlp.getAggregates() != null && !qlp.getAggregates().isEmpty()) {
      java.util.List<com.dafei1288.jimsql.server.plan.logical.AggregateSpec> specs = qlp.getAggregates();
      java.util.List<com.dafei1288.jimsql.common.meta.JqColumn> gcols = qlp.getGroupByColumns();
      boolean hasGroup = (gcols != null && !gcols.isEmpty());
      // state per group
      class AggState {
        java.util.Map<String,String> groupVals = new java.util.LinkedHashMap<>();
        long[] count = new long[specs.size()];
        java.math.BigDecimal[] sum = new java.math.BigDecimal[specs.size()];
        java.math.BigDecimal[] avgSum = new java.math.BigDecimal[specs.size()];
        long[] avgCnt = new long[specs.size()];
        String[] minStr = new String[specs.size()];
        String[] maxStr = new String[specs.size()];
        java.math.BigDecimal[] minNum = new java.math.BigDecimal[specs.size()];
        java.math.BigDecimal[] maxNum = new java.math.BigDecimal[specs.size()];
      }
      java.util.Map<String,AggState> groups = new java.util.LinkedHashMap<>();
      for (java.util.Map<String,String> r : fullRows) {
        String key;
        AggState st;
        if (hasGroup) {
          StringBuilder kb = new StringBuilder();
          java.util.Map<String,String> gmap = new java.util.LinkedHashMap<>();
          for (com.dafei1288.jimsql.common.meta.JqColumn c : gcols) {
            String col = normalizeColumn(c.getColumnName());
            String v = getCaseInsensitive(r, col);
            gmap.put(c.getColumnName(), v);
            kb.append('\u0001').append(v==null?"":v);
          }
          key = kb.toString();
          st = groups.get(key);
          if (st == null) { st = new AggState(); st.groupVals.putAll(gmap); groups.put(key, st); }
        } else {
          key = "__ALL__"; st = groups.get(key); if (st == null) { st = new AggState(); groups.put(key, st);} }
        // apply specs
        for (int i = 0; i < specs.size(); i++) {
          com.dafei1288.jimsql.server.plan.logical.AggregateSpec sp = specs.get(i);
          String col = sp.getColumn();
          String label;
          if (sp.getAlias() != null && !sp.getAlias().isEmpty()) label = sp.getAlias();
          else {
            switch (sp.getType()) {
              case COUNT: label = (col == null || "*".equals(col)) ? "count" : ("count_" + normalizeColumn(col)); break;
              case SUM:   label = "sum_" + normalizeColumn(col); break;
              case AVG:   label = "avg_" + normalizeColumn(col); break;
              case MIN:   label = "min_" + normalizeColumn(col); break;
              case MAX:   label = "max_" + normalizeColumn(col); break;
              default:    label = "agg"; break;
            }
          }
          switch (sp.getType()) {
            case COUNT: {
              if (col == null || "*".equals(col)) { st.count[i]++; }
              else {
                String v = getCaseInsensitive(r, normalizeColumn(col));
                if (v != null && !v.isEmpty()) st.count[i]++;
              }
              break; }
            case SUM: {
              String v = getCaseInsensitive(r, normalizeColumn(col));
              if (v != null && !v.trim().isEmpty()) {
                try { java.math.BigDecimal b = new java.math.BigDecimal(v.trim()); st.sum[i] = (st.sum[i]==null? b : st.sum[i].add(b)); } catch (Exception ignore) {}
              }
              break; }
            case AVG: {
              String v = getCaseInsensitive(r, normalizeColumn(col));
              if (v != null && !v.trim().isEmpty()) {
                try { java.math.BigDecimal b = new java.math.BigDecimal(v.trim()); st.avgSum[i] = (st.avgSum[i]==null? b : st.avgSum[i].add(b)); st.avgCnt[i]++; } catch (Exception ignore) {}
              }
              break; }
            case MIN: {
              String v = getCaseInsensitive(r, normalizeColumn(col));
              int t = columnSqlType(jqTable, normalizeColumn(col));
              if (v != null && !v.isEmpty()) {
                if (isNumericType(t)) {
                  try { java.math.BigDecimal b = new java.math.BigDecimal(v.trim()); if (st.minNum[i]==null || b.compareTo(st.minNum[i])<0) st.minNum[i]=b; } catch (Exception ignore) {}
                } else {
                  if (st.minStr[i]==null || v.compareTo(st.minStr[i])<0) st.minStr[i]=v;
                }
              }
              break; }
            case MAX: {
              String v = getCaseInsensitive(r, normalizeColumn(col));
              int t = columnSqlType(jqTable, normalizeColumn(col));
              if (v != null && !v.isEmpty()) {
                if (isNumericType(t)) {
                  try { java.math.BigDecimal b = new java.math.BigDecimal(v.trim()); if (st.maxNum[i]==null || b.compareTo(st.maxNum[i])>0) st.maxNum[i]=b; } catch (Exception ignore) {}
                } else {
                  if (st.maxStr[i]==null || v.compareTo(st.maxStr[i])>0) st.maxStr[i]=v;
                }
              }
              break; }
          }
        }
      }
      // build aggregated rows
      java.util.List<java.util.Map<String,String>> aggRows = new java.util.ArrayList<>();
      for (java.util.Map.Entry<String,AggState> e : groups.entrySet()) {
        AggState st = e.getValue();
        java.util.LinkedHashMap<String,String> out = new java.util.LinkedHashMap<>();
        // group-by columns first
        if (hasGroup) { out.putAll(st.groupVals); }
        for (int i = 0; i < specs.size(); i++) {
          com.dafei1288.jimsql.server.plan.logical.AggregateSpec sp = specs.get(i);
          String col = sp.getColumn();
          String label;
          if (sp.getAlias() != null && !sp.getAlias().isEmpty()) label = sp.getAlias();
          else {
            switch (sp.getType()) {
              case COUNT: label = (col == null || "*".equals(col)) ? "count" : ("count_" + normalizeColumn(col)); break;
              case SUM:   label = "sum_" + normalizeColumn(col); break;
              case AVG:   label = "avg_" + normalizeColumn(col); break;
              case MIN:   label = "min_" + normalizeColumn(col); break;
              case MAX:   label = "max_" + normalizeColumn(col); break;
              default:    label = "agg"; break;
            }
          }
          switch (sp.getType()) {
            case COUNT: out.put(label, String.valueOf(st.count[i])); break;
            case SUM:   out.put(label, st.sum[i]==null? null : st.sum[i].toPlainString()); break;
            case AVG:   out.put(label, (st.avgCnt[i]==0||st.avgSum[i]==null)? null : st.avgSum[i].divide(new java.math.BigDecimal(st.avgCnt[i]), java.math.MathContext.DECIMAL64).toPlainString()); break;
            case MIN: {
              int t = columnSqlType(jqTable, normalizeColumn(col));
              if (isNumericType(t)) out.put(label, st.minNum[i]==null? null : st.minNum[i].toPlainString()); else out.put(label, st.minStr[i]);
              break; }
            case MAX: {
              int t = columnSqlType(jqTable, normalizeColumn(col));
              if (isNumericType(t)) out.put(label, st.maxNum[i]==null? null : st.maxNum[i].toPlainString()); else out.put(label, st.maxStr[i]);
              break; }
          }
        }
        aggRows.add(out);
      }
      fullRows = aggRows;
      // After aggregation, header becomes selected result labels
      java.util.LinkedHashMap<String,JqColumnResultSetMetadata> meta = optimizeQueryLogicalPlan.getJqColumnResultSetMetadataList();
      header = new java.util.ArrayList<>(meta.keySet());
      // HAVING: if present, evaluate on aggregated rows using a temporary JqTable metadata
      String having = qlp.getHavingExpression();
      if (having != null && !having.trim().isEmpty()) {
        // build temp JqTable for type info from metadata
        com.dafei1288.jimsql.common.meta.JqTable temp = new com.dafei1288.jimsql.common.meta.JqTable();
        java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> cmap = new java.util.LinkedHashMap<>();
        for (JqColumnResultSetMetadata m : meta.values()) {
          com.dafei1288.jimsql.common.meta.JqColumn jc = new com.dafei1288.jimsql.common.meta.JqColumn();
          jc.setColumnName(m.getLabelName()); jc.setColumnType(m.getColumnType()); jc.setColumnClazzType(m.getClazz()); jc.setTable(temp);
          cmap.put(m.getLabelName(), jc);
        }
        temp.setJqTableLinkedHashMap(cmap);
        final com.dafei1288.jimsql.server.plan.physical.WhereEvaluator.Node ex = com.dafei1288.jimsql.server.plan.physical.WhereEvaluator.parse(having);
        final com.dafei1288.jimsql.common.meta.JqTable _tmp = temp; fullRows = fullRows.stream().filter(r -> ex.eval(r, _tmp)).collect(java.util.stream.Collectors.toList());
      }
    }// ORDER BY
    if (qlp.getOrderBy() != null && !qlp.getOrderBy().isEmpty()) {
      java.util.List<String> header1 = new java.util.ArrayList<>(header); boolean canSort = qlp.getOrderBy().stream().allMatch(oi -> headerContains(header1, oi.getColumn().getColumnName()));
      if (canSort) {
        Comparator<Map<String,String>> cmp = buildComparator(qlp.getOrderBy(), jqTable);
        fullRows.sort(cmp);
      }
    }

    // LIMIT/OFFSET
    int offset = qlp.getOffset() == null ? 0 : Math.max(0, qlp.getOffset());
    Integer limit = qlp.getLimit();
    int from = Math.min(offset, fullRows.size());
    int to = (limit == null) ? fullRows.size() : Math.min(fullRows.size(), from + Math.max(0, limit));
    List<Map<String,String>> finalRows = (from <= to) ? fullRows.subList(from, to) : new ArrayList<>();

    // Project selected columns only, then write
    LinkedHashMap<String,JqColumnResultSetMetadata> rsMeta = optimizeQueryLogicalPlan.getJqColumnResultSetMetadataList();
    Set<String> selectedCols = rsMeta.keySet();

    for (Map<String,String> full : finalRows) {
      LinkedHashMap<String,Object> datatrans = new LinkedHashMap<>();
      for (String key : selectedCols) {
        datatrans.put(key, full.get(key));
      }
      RowData rowData = new RowData();
      rowData.setNext(true);
      rowData.setDatas(datatrans);
      ctx.writeAndFlush(rowData);
    }
  }

  // --------------------- Helpers ---------------------

  private static boolean headerContains(List<String> header, String col) {
    String c = normalizeColumn(col);
    for (String h : header) {
      if (h.equalsIgnoreCase(c)) return true;
    }
    return false;
  }

  private static String normalizeColumn(String c) {
    if (c == null) return null;
    c = stripQuotes(c);
    int dot = c.lastIndexOf('.') ;
    if (dot >= 0) c = c.substring(dot+1);
    return c;
  }

  private static String stripQuotes(String s) {
    if (s == null || s.length() < 2) return s;
    char f = s.charAt(0), l = s.charAt(s.length()-1);
    if ((f == '`' && l == '`') || (f == '"' && l == '"')) {
      return s.substring(1, s.length()-1);
    }
    return s;
  }

  private static Comparator<Map<String,String>> buildComparator(List<OrderItem> items, JqTable jt) {
    // Compose comparator per order item
    Comparator<Map<String,String>> cmp = (a,b) -> 0;
    for (OrderItem oi : items) {
      String col = normalizeColumn(oi.getColumn().getColumnName());
      int sqlType = columnSqlType(jt, col);
      Comparator<Map<String,String>> c = (m1, m2) -> compareValues(getCaseInsensitive(m1, col), getCaseInsensitive(m2, col), sqlType);
      if (!oi.isAsc()) c = c.reversed();
      cmp = cmp.thenComparing(c);
    }
    return cmp;
  }

  private static int columnSqlType(JqTable jt, String col) {
    for (String k : jt.getJqTableLinkedHashMap().keySet()) { if (k.equalsIgnoreCase(col)) { JqColumn jc = jt.getJqTableLinkedHashMap().get(k); if (jc != null) return jc.getColumnType(); } } return java.sql.Types.VARCHAR;
  }

  private static int compareValues(String v1, String v2, int sqlType) {
    if (Objects.equals(v1, v2)) return 0;
    if (v1 == null) return -1;
    if (v2 == null) return 1;
    switch (sqlType) {
      case java.sql.Types.INTEGER:
      case java.sql.Types.BIGINT:
      case java.sql.Types.SMALLINT:
      case java.sql.Types.TINYINT:
      case java.sql.Types.DOUBLE:
      case java.sql.Types.FLOAT:
      case java.sql.Types.DECIMAL:
      case java.sql.Types.NUMERIC:
        try {
          BigDecimal b1 = new BigDecimal(v1.trim());
          BigDecimal b2 = new BigDecimal(v2.trim());
          return b1.compareTo(b2);
        } catch (Exception ignore) { /* fall back to string */ }
      default:
        return v1.compareTo(v2);
    }
  }

  private static boolean evalPredicates(Map<String,String> row, JqTable jt, List<Predicate> preds) {
    for (Predicate p : preds) {
      String col = normalizeColumn(p.column);
      String raw = null; for (String k : row.keySet()) { if (k.equalsIgnoreCase(col)) { raw = row.get(k); break; } }
      if (!evalOne(raw, p, jt)) return false;
    }
    return true;
  }

  private static boolean evalOne(String raw, Predicate p, JqTable jt) {
    if (p == null) return true;
    String col = normalizeColumn(p.column);
    int sqlType = columnSqlType(jt, col);
    if (raw == null) raw = "";

    if (isNumericType(sqlType) && p.literalNumeric != null) {
      try {
        BigDecimal left = new BigDecimal(raw.trim());
        BigDecimal right = p.literalNumeric;
        int c = left.compareTo(right);
        return switchOp(c, p.op);
      } catch (Exception e) {
        // fall back to string compare
      }
    }
    int c = raw.compareTo(p.literalString);
    return switchOp(c, p.op);
  }

  private static boolean isNumericType(int t) {
    switch (t) {
      case java.sql.Types.INTEGER:
      case java.sql.Types.BIGINT:
      case java.sql.Types.SMALLINT:
      case java.sql.Types.TINYINT:
      case java.sql.Types.DOUBLE:
      case java.sql.Types.FLOAT:
      case java.sql.Types.DECIMAL:
      case java.sql.Types.NUMERIC:
        return true;
      default:
        return false;
    }
  }

  private static boolean switchOp(int cmp, String op) {
    if ("=".equals(op)) return cmp == 0;
    if ("!=".equals(op)) return cmp != 0;
    if (">".equals(op)) return cmp > 0;
    if (">=".equals(op)) return cmp >= 0;
    if ("<".equals(op)) return cmp < 0;
    if ("<=".equals(op)) return cmp <= 0;
    return false;
  }

  // Very small parser for: col op literal AND col op literal ...
  private static List<Predicate> parseWhere(String where) {
    List<String> parts = splitByAnd(where);
    List<Predicate> res = new ArrayList<>();
    for (String p : parts) {
      Predicate pr = parsePredicate(p.trim());
      if (pr != null) res.add(pr);
    }
    return res;
  }

  private static List<String> splitByAnd(String s) {
    List<String> out = new ArrayList<>();
    StringBuilder buf = new StringBuilder();
    boolean inStr = false;
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch == '\'' ) {
        inStr = !inStr;
        buf.append(ch);
        continue;
      }
      if (!inStr && i + 3 <= s.length()) {
        String sub = s.substring(i, Math.min(i+3, s.length()));
        if (sub.equalsIgnoreCase("AND")) {
          // flush buffer and skip token
          out.add(buf.toString());
          buf.setLength(0);
          i += 2; // skip ND
          continue;
        }
      }
      buf.append(ch);
    }
    if (buf.length() > 0) out.add(buf.toString());
    return out.stream().filter(t -> t != null && !t.trim().isEmpty()).collect(Collectors.toList());
  }

  private static Predicate parsePredicate(String s) {
    // expected: <col> <op> <literal>
    String[] ops = new String[]{">=","<=","!=","=",">","<"};
    String opFound = null;
    int pos = -1;
    for (String op : ops) {
      int idx = indexOfOp(s, op);
      if (idx >= 0) { opFound = op; pos = idx; break; }
    }
    if (opFound == null) return null;
    String lhs = s.substring(0, pos).trim();
    String rhs = s.substring(pos + opFound.length()).trim();
    Predicate p = new Predicate();
    p.column = lhs;
    p.op = opFound;
    if (rhs.startsWith("'")) {
      // string literal (single quotes)
      int end = rhs.lastIndexOf('\'');
      String content = (end > 0) ? rhs.substring(1, end) : rhs.substring(1);
      p.literalString = content;
      p.literalNumeric = null;
    } else {
      p.literalString = rhs;
      try { p.literalNumeric = new BigDecimal(rhs); } catch (Exception e) { p.literalNumeric = null; }
    }
    return p;
  }

  private static int indexOfOp(String s, String op) {
    boolean inStr = false;
    for (int i = 0; i <= s.length() - op.length(); i++) {
      char ch = s.charAt(i);
      if (ch == '\'') inStr = !inStr;
      if (!inStr && s.regionMatches(true, i, op, 0, op.length())) return i;
    }
    return -1;
  }

  private static class Predicate {
    String column; // may contain alias or quotes
    String op;     // =, !=, >, >=, <, <=
    String literalString;
    BigDecimal literalNumeric;
  }
  private static String getCaseInsensitive(java.util.Map<String,String> row, String col) {
      if (row == null || col == null) return null;
      for (String k : row.keySet()) {
          if (k.equalsIgnoreCase(col)) return row.get(k);
      }
      return null;
  }
  // -------------- JOIN helpers --------------
  private static String stripParens(String s){ if (s==null) return null; s=s.trim(); while (s.startsWith("(") && s.endsWith(")")) { s = s.substring(1, s.length()-1).trim(); } return s; }
  private static String stripTailSemi(String s){ if (s==null) return null; int i=s.length()-1; while (i>=0){ char c=s.charAt(i); if (c==';'||c==' '||c=='\t'||c=='\r'||c=='\n'){ i--; } else break; } return s.substring(0, i+1); }  private static java.util.List<String[]> parseJoinOnEquals(String onExpr) {
    java.util.List<String[]> res = new java.util.ArrayList<>();
    if (onExpr == null) return res;
    String s = onExpr.trim();
    // split by AND (case-insensitive), ignoring quotes
    java.util.List<String> parts = new java.util.ArrayList<>();
    StringBuilder buf = new StringBuilder(); boolean inS=false; char q=0;
    for (int i=0;i<s.length();i++){
      char c=s.charAt(i);
      if (inS){ if (c==q) inS=false; buf.append(c); continue; }
      if (c=='\''||c=='"'){ inS=true; q=c; buf.append(c); continue; }
      if (i+3<=s.length() && s.regionMatches(true,i,"AND",0,3)) { parts.add(buf.toString()); buf.setLength(0); i+=2; continue; }
      buf.append(c);
    }
    if (buf.length()>0) parts.add(buf.toString());
    for (String p : parts) {
      String[] lr = p.split("=");
      if (lr.length==2) {
        String l = lr[0].trim(); String r = lr[1].trim();
        res.add(new String[]{stripTailSemi(stripQuotes(stripParens(l))), stripTailSemi(stripQuotes(stripParens(r)))});
      }
    }
    return res;
  }
  private static String stripQual(String id){ id=id.replace("`","").replace("\"",""); int d=id.lastIndexOf('.'); if (d>=0) return id.substring(d+1).trim(); return id.trim(); }
  private static String buildKey(java.util.Map<String,String> row, java.util.List<String> header, java.util.List<String[]> eqs, boolean isLeft){
    StringBuilder sb=new StringBuilder();
    for (String[] lr : eqs){ String col = isLeft? lr[0]: lr[1]; String v = getCaseInsensitive(row, stripQual(col)); sb.append('\u0001').append(v==null?"":v); }
    return sb.toString();
  }
  private static String qualifier(String id){ if (id==null) return ""; String s=stripQuotes(id).trim(); int d=s.lastIndexOf('.'); return d>=0? s.substring(0,d).trim():""; }
  private static String simple(String id){ if (id==null) return null; String s=stripQuotes(id).trim(); int d=s.lastIndexOf('.'); return d>=0? s.substring(d+1).trim():s; }
  private static boolean headerHasSimple(java.util.List<String> header, String simple){ if (simple==null) return false; for(String h: header){ String hs=h; int d=hs.lastIndexOf('.'); if(d>=0) hs=hs.substring(d+1); if (hs.equalsIgnoreCase(simple)) return true; } return false; }
  private static String buildJoinKey(java.util.Map<String,String> row, java.util.List<String> leftHeader, java.util.List<String> rightHeader, String rightAlias, boolean forLeft, java.util.List<String[]> eqs){
    StringBuilder sb=new StringBuilder();
    String rAlias = (rightAlias==null? "": rightAlias);
    for (String[] lr : eqs){
      String L=lr[0], R=lr[1];
      String qL=qualifier(L), qR=qualifier(R);
      String sL=simple(L), sR=simple(R);
      String pick=null;
      if (forLeft){
        if (qL.equalsIgnoreCase(rAlias)) pick=sR;
        else if (qR.equalsIgnoreCase(rAlias)) pick=sL;
        else if (headerHasSimple(leftHeader,sL) && !headerHasSimple(rightHeader,sL)) pick=sL;
        else if (headerHasSimple(leftHeader,sR) && !headerHasSimple(rightHeader,sR)) pick=sR;
        else if (headerHasSimple(leftHeader,sL) && !headerHasSimple(leftHeader,sR)) pick=sL;
        else if (headerHasSimple(leftHeader,sR) && !headerHasSimple(leftHeader,sL)) pick=sR;
      } else {
        if (qL.equalsIgnoreCase(rAlias)) pick=sL;
        else if (qR.equalsIgnoreCase(rAlias)) pick=sR;
        else if (headerHasSimple(rightHeader,sL) && !headerHasSimple(leftHeader,sL)) pick=sL;
        else if (headerHasSimple(rightHeader,sR) && !headerHasSimple(leftHeader,sR)) pick=sR;
        else if (headerHasSimple(rightHeader,sL) && !headerHasSimple(rightHeader,sR)) pick=sL;
        else if (headerHasSimple(rightHeader,sR) && !headerHasSimple(rightHeader,sL)) pick=sR;
      }
      if (pick==null){
        String vL=getCaseInsensitive(row,sL);
        String vR=getCaseInsensitive(row,sR);
        pick=(vL!=null && !vL.isEmpty())? sL : ((vR!=null && !vR.isEmpty())? sR : sL);
      }
      String v=getCaseInsensitive(row,pick);
      sb.append('\u0001').append(v==null? "": v);
    }
    return sb.toString();
  }  private static String buildKey2(java.util.Map<String,String> row, java.util.List<String> header, java.util.List<String[]> eqs, boolean isLeft){
    StringBuilder sb=new StringBuilder();
    for (String[] lr : eqs){
      String a = stripQual(lr[0]);
      String b = stripQual(lr[1]);
      String pick;
      if (isLeft) {
        pick = headerContains(header, a) ? a : (headerContains(header, b) ? b : a);
      } else {
        pick = headerContains(header, b) ? b : (headerContains(header, a) ? a : b);
      }
      String v = getCaseInsensitive(row, pick);
      sb.append('\u0001').append(v==null?"":v);
    }
    return sb.toString();
  }}


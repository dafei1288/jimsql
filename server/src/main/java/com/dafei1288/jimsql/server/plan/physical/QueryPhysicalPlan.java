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

    // Read file lines
    List<String> datas = Files.readLines(jqTable.getBasepath(), Charset.defaultCharset());
    if (datas.isEmpty()) return;

    // Header -> column order
    List<String> header = new ArrayList<>(jqTable.getJqTableLinkedHashMap().keySet());

    // Load all rows into fullRow list (col->string value)
    List<Map<String,String>> fullRows = new ArrayList<>();
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
      fullRows.add(full);
    }

    // WHERE filter (enhanced: AND/OR, parentheses, IS NULL, LIKE, IN)
    String where = qlp.getWhereExpression();
    if (where != null && !where.trim().isEmpty()) {
      List<Predicate> preds = parseWhere(where);
      boolean canApply = preds.stream().allMatch(p -> headerContains(header, p.column));
      if (canApply) {
        fullRows = fullRows.stream().filter(r -> evalPredicates(r, jqTable, preds)).collect(java.util.stream.Collectors.toList());
      }
    }

    // ORDER BY
    if (qlp.getOrderBy() != null && !qlp.getOrderBy().isEmpty()) {
      boolean canSort = qlp.getOrderBy().stream().allMatch(oi -> headerContains(header, oi.getColumn().getColumnName()));
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
}

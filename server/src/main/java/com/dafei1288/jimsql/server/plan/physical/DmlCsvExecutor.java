package com.dafei1288.jimsql.server.plan.physical;

import com.dafei1288.jimsql.common.Utils;
import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.instance.ServerMetadata;
import com.dafei1288.jimsql.server.plan.logical.DeleteLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.UpdateLogicalPlan;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// Minimal CSV executor for UPDATE / DELETE. Mirrors QueryPhysicalPlan filter semantics.
public final class DmlCsvExecutor {  public static int executeInsert(String dbName, com.dafei1288.jimsql.server.plan.logical.InsertLogicalPlan plan) throws IOException {
    if (plan == null || plan.getTable() == null) return 0;
    String table = plan.getTable().getTableName();
    ServerMetadata sm = ServerMetadata.getInstance();
    JqTable jt = sm.fetchTableByName(dbName, table);
    if (jt == null) return 0;

    List<String> headerLines = Files.readAllLines(jt.getBasepath().toPath(), Charset.defaultCharset());
    if (headerLines.isEmpty()) return 0;
    String headerLine = headerLines.get(0);
    String[] header = headerLine.split(Utils.COLUMN_SPILTOR, -1);
    java.util.Map<String,Integer> hmap = new java.util.HashMap<>();
    for (int i=0;i<header.length;i++) hmap.put(header[i].toLowerCase(java.util.Locale.ROOT), i);

    List<String> toAppend = new ArrayList<>();
    List<String> cols = plan.getColumns();
    boolean hasCols = (cols != null && !cols.isEmpty());
    for (List<String> row : plan.getRows()) {
      String[] cells = new String[header.length];
      for (int k=0;k<cells.length;k++) cells[k] = "";
      if (hasCols) {
        for (int c=0;c<cols.size();c++) {
          String col = cols.get(c);
          Integer idx = hmap.get(col.toLowerCase(java.util.Locale.ROOT));
          if (idx == null) throw new IOException("unknown column: "+col);
          String v = (c < row.size()) ? row.get(c) : "";
          if (v != null && v.contains(Utils.COLUMN_SPILTOR)) throw new IOException("value contains column delimiter");
          cells[idx] = (v==null?"":v);
        }
      } else {
        for (int c=0;c<Math.min(row.size(), header.length); c++) {
          String v = row.get(c);
          if (v != null && v.contains(Utils.COLUMN_SPILTOR)) throw new IOException("value contains column delimiter");
          cells[c] = (v==null?"":v);
        }
      }
      toAppend.add(String.join(Utils.COLUMN_SPILTOR, cells));
    }
    Files.write(jt.getBasepath().toPath(), toAppend, Charset.defaultCharset(), java.nio.file.StandardOpenOption.APPEND);
    return toAppend.size();
  }
  private DmlCsvExecutor() {}

  public static int executeUpdate(String dbName, UpdateLogicalPlan plan) throws IOException {
    if (plan == null || plan.getTable() == null) return 0;
    String table = plan.getTable().getTableName();
    ServerMetadata sm = ServerMetadata.getInstance();
    JqTable jt = sm.fetchTableByName(dbName, table);
    if (jt == null) return 0;

    List<String> lines = Files.readAllLines(jt.getBasepath().toPath(), Charset.defaultCharset());
    if (lines.isEmpty()) return 0;
    String headerLine = lines.get(0);
    String[] header = headerLine.split(Utils.COLUMN_SPILTOR, -1);

    List<String> out = new ArrayList<>(lines.size());
    out.add(headerLine);

    List<Predicate> preds = parseWhere(plan.getWhereExpression());

    int updated = 0;
    for (int i = 1; i < lines.size(); i++) {
      String line = lines.get(i);
      String[] cells = line.split(Utils.COLUMN_SPILTOR, -1);
      Map<String,String> row = rowMap(header, cells);
      boolean match = preds.isEmpty() || evalPredicates(row, jt, preds);
      if (match) {
        // apply assignments
        LinkedHashMap<String,String> sets = plan.getAssignments();
        for (Map.Entry<String,String> e : sets.entrySet()) {
          String col = normalizeColumn(e.getKey());
          int idx = indexOfHeader(header, col);
          if (idx >= 0) {
            String val = e.getValue();
            cells = ensureLength(cells, header.length);
            cells[idx] = (val == null) ? "" : val;
          }
        }
        updated++;
      }
      out.add(String.join(Utils.COLUMN_SPILTOR, padTo(cells, header.length)));
    }

    writeBackAtomic(jt.getBasepath().toPath(), out);
    return updated;
  }

  public static int executeDelete(String dbName, DeleteLogicalPlan plan) throws IOException {
    if (plan == null || plan.getTable() == null) return 0;
    String table = plan.getTable().getTableName();
    ServerMetadata sm = ServerMetadata.getInstance();
    JqTable jt = sm.fetchTableByName(dbName, table);
    if (jt == null) return 0;

    List<String> lines = Files.readAllLines(jt.getBasepath().toPath(), Charset.defaultCharset());
    if (lines.isEmpty()) return 0;
    String headerLine = lines.get(0);
    String[] header = headerLine.split(Utils.COLUMN_SPILTOR, -1);

    List<String> out = new ArrayList<>(lines.size());
    out.add(headerLine);

    List<Predicate> preds = parseWhere(plan.getWhereExpression());

    int deleted = 0;
    for (int i = 1; i < lines.size(); i++) {
      String line = lines.get(i);
      String[] cells = line.split(Utils.COLUMN_SPILTOR, -1);
      Map<String,String> row = rowMap(header, cells);
      boolean match = preds.isEmpty() || evalPredicates(row, jt, preds);
      if (match) { deleted++; continue; }
      out.add(String.join(Utils.COLUMN_SPILTOR, padTo(cells, header.length)));
    }

    writeBackAtomic(jt.getBasepath().toPath(), out);
    return deleted;
  }

  // ---------------- helpers ----------------
  private static Map<String,String> rowMap(String[] header, String[] cells) {
    LinkedHashMap<String,String> m = new LinkedHashMap<>();
    for (int i=0;i<header.length;i++) {
      String key = header[i];
      String val = (i<cells.length) ? cells[i] : "";
      m.put(key, val);
    }
    return m;
  }

  private static int indexOfHeader(String[] header, String col) {
    for (int i=0;i<header.length;i++) { if (header[i].equalsIgnoreCase(col)) return i; }
    return -1;
  }

  private static String[] ensureLength(String[] cells, int n) {
    if (cells.length >= n) return cells;
    String[] out = new String[n];
    System.arraycopy(cells, 0, out, 0, cells.length);
    for (int i=cells.length;i<n;i++) out[i] = "";
    return out;
  }

  private static String[] padTo(String[] cells, int n) {
    return ensureLength(cells, n);
  }

  private static void writeBackAtomic(Path target, List<String> lines) throws IOException {
    File f = target.toFile();
    File tmp = new File(f.getParentFile(), f.getName()+".tmp");
    Files.write(tmp.toPath(), lines, Charset.defaultCharset());
    Files.move(tmp.toPath(), target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  private static String normalizeColumn(String c) {
    if (c == null) return null;
    c = stripQuotes(c);
    int dot = c.lastIndexOf('.');
    if (dot >= 0) c = c.substring(dot+1);
    return c;
  }

  private static String stripQuotes(String s) {
    if (s == null || s.length() < 2) return s;
    char f = s.charAt(0), l = s.charAt(s.length()-1);
    if ((f == '`' && l == '`') || (f == '"' && l == '"')) return s.substring(1, s.length()-1);
    return s;
  }

  // WHERE evaluation (same as QueryPhysicalPlan simple AND comparisons)
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
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.SMALLINT:
      case Types.TINYINT:
      case Types.DOUBLE:
      case Types.FLOAT:
      case Types.DECIMAL:
      case Types.NUMERIC:
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

  private static List<Predicate> parseWhere(String where) {
    List<Predicate> res = new ArrayList<>();
    if (where == null || where.trim().isEmpty()) return res;
    List<String> parts = splitByAnd(where);
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
      if (ch == '\'') { inStr = !inStr; buf.append(ch); continue; }
      if (!inStr && i + 3 <= s.length()) {
        String sub = s.substring(i, Math.min(i+3, s.length()));
        if (sub.equalsIgnoreCase("AND")) { out.add(buf.toString()); buf.setLength(0); i += 2; continue; }
      }
      buf.append(ch);
    }
    if (buf.length() > 0) out.add(buf.toString());
    return out;
  }

  private static Predicate parsePredicate(String s) {
    String[] ops = new String[]{">=","<=","!=","=",">","<"};
    String opFound = null; int pos = -1;
    for (String op : ops) { int idx = indexOfOp(s, op); if (idx >= 0) { opFound = op; pos = idx; break; } }
    if (opFound == null) return null;
    String lhs = s.substring(0, pos).trim();
    String rhs = s.substring(pos + opFound.length()).trim();
    Predicate p = new Predicate(); p.column = lhs; p.op = opFound;
    if (rhs.startsWith("'")) {
      int end = rhs.lastIndexOf('\''); String content = (end > 0) ? rhs.substring(1, end) : rhs.substring(1);
      p.literalString = content; p.literalNumeric = null;
    } else { p.literalString = rhs; try { p.literalNumeric = new BigDecimal(rhs); } catch (Exception e) { p.literalNumeric = null; } }
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

  private static int columnSqlType(JqTable jt, String col) {
    for (String k : jt.getJqTableLinkedHashMap().keySet()) {
      if (k.equalsIgnoreCase(col)) {
        JqColumn jc = jt.getJqTableLinkedHashMap().get(k);
        if (jc != null) return jc.getColumnType();
      }
    }
    return Types.VARCHAR;
  }

  private static class Predicate {
    String column;
    String op;
    String literalString;
    BigDecimal literalNumeric;
  }
}

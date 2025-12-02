package com.dafei1288.jimsql.cli.render;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class TableRenderer {
  private TableRenderer() {}

  public static String render(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    if (md == null) throw new SQLException("No metadata");
    int cols = md.getColumnCount();
    String[] headers = new String[cols];
    int[] widths = new int[cols];
    for (int i = 1; i <= cols; i++) {
      headers[i-1] = safeLabel(md, i);
      widths[i-1] = headers[i-1] != null ? headers[i-1].length() : 4;
    }
    List<String[]> rows = new ArrayList<>();
    while (rs.next()) {
      String[] row = new String[cols];
      for (int i = 1; i <= cols; i++) {
        String v = rs.getString(headers[i-1]);
        if (v == null) v = "NULL";
        row[i-1] = v;
        widths[i-1] = Math.max(widths[i-1], v.length());
      }
      rows.add(row);
    }
    StringBuilder sb = new StringBuilder();
    String hline = line(widths);
    sb.append(Color.dim(hline)).append('\n');
    // header
    sb.append('|');
    for (int i = 0; i < cols; i++) {
      sb.append(' ').append(pad(Color.cyan(Color.bold(headers[i])), widths[i])).append(' ').append('|');
    }
    sb.append('\n');
    sb.append(Color.dim(hline)).append('\n');
    // rows
    for (String[] row : rows) {
      sb.append('|');
      for (int i = 0; i < cols; i++) {
        sb.append(' ').append(pad(row[i], widths[i])).append(' ').append('|');
      }
      sb.append('\n');
    }
    sb.append(Color.dim(hline)).append('\n');
    sb.append(Color.green("Rows: ")).append(rows.size()).append('\n');
    return sb.toString();
  }

  private static String line(int[] widths) {
    StringBuilder sb = new StringBuilder();
    sb.append('+');
    for (int w : widths) {
      for (int i = 0; i < w + 2; i++) sb.append('-');
      sb.append('+');
    }
    return sb.toString();
  }

  private static String pad(String s, int w) {
    if (s.length() >= w) return s;
    StringBuilder sb = new StringBuilder(s);
    while (sb.length() < w) sb.append(' ');
    return sb.toString();
  }

  private static String safeLabel(ResultSetMetaData md, int i) {
    try {
      String l = md.getColumnLabel(i);
      if (l == null || l.isEmpty()) l = md.getColumnName(i);
      if (l == null || l.isEmpty()) l = "c" + i;
      return l;
    } catch (Exception e) {
      try {
        String n = md.getColumnName(i);
        return (n == null || n.isEmpty()) ? ("c" + i) : n;
      } catch (Exception ex) {
        return "c" + i;
      }
    }
  }
}

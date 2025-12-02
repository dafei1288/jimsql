package com.dafei1288.jimsql.cli;

import com.dafei1288.jimsql.cli.io.CsvIO;
import com.dafei1288.jimsql.cli.io.JsonIO;
import com.dafei1288.jimsql.cli.render.Color;
import com.dafei1288.jimsql.cli.render.TableRenderer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.File;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Repl {
  private String url;
  private String host;
  private Integer port;
  private String db;
  private String user;
  private String password;
  private Main.Format format;
  private boolean timing = true;

  private Connection conn;

  public Repl(String url, String host, Integer port, String db, String user, String password, Main.Format format) {
    this.url = url;
    this.host = host;
    this.port = port;
    this.db = db;
    this.user = user;
    this.password = password;
    this.format = format != null ? format : Main.Format.table;
  }

  public int run() {
    try {
      Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
      Terminal terminal = TerminalBuilder.builder().encoding(StandardCharsets.UTF_8).system(true).build();
      LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();
      String prompt = Color.blue("jimsql> ");
      String cont = Color.dim(" ....> ");
      StringBuilder buf = new StringBuilder();

      // Try initial connection
      try { tryConnect(resolveUrl()); } catch (Exception ce) { println(com.dafei1288.jimsql.cli.render.Color.yellow("Not connected. Type \\connect <jdbc-url> or set JIMSQL_URL (tip: add ?protocol=legacy|jspv1)")); }

      for (;;) {
        String line;
        try {
          line = reader.readLine(buf.length() == 0 ? prompt : cont);
        } catch (UserInterruptException e) {
          // Ctrl-C clears buffer
          buf.setLength(0);
          continue;
        } catch (EndOfFileException e) {
          break;
        }
        if (line == null) break;
        line = line.trim();
        if (line.isEmpty()) continue;

        if (line.startsWith("\\")) { // meta command
          try {
              if (handleMeta(line)) {
                  continue;
              } else {
                  println(Color.red("Unknown command: ") + line);
                  continue;
              }
          } catch (EndOfFileException e) {
              break;
          }
      }


        buf.append(line).append('\n');
        if (isComplete(buf)) {
          String sql = buf.toString();
          buf.setLength(0);
          execute(sql);
        }
      }
      println("bye");
      return 0;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return 1;
    } finally {
      try { if (conn != null) conn.close(); } catch (Exception ignore) {}
    }
  }

  private boolean handleMeta(String line) throws Exception {
    String[] parts = line.split("\\s+");
    String cmd = parts[0];
    switch (cmd) {
      case "\\q":
      case "\\quit":
      case "\\exit":
        throw new EndOfFileException("quit");
      case "\\help":
      case "\\?":
        println("Meta commands:");
        println("  \\q                    quit");
        println("  \\help                 show this help");
        println("  \\timing on|off        toggle timing");
        println("  \\set format <fmt>    set output format: table|csv|json");
        println("  \\connect <jdbc-url>  connect to another database");
        println("  \\import csv <file> <table>");
        println("  \\import json <file> <table>");
        println("  \\export csv <table> <file>");
        println("  \\export json <table> <file>");
        return true;
      case "\\timing":
        if (parts.length >= 2) {
          this.timing = parts[1].equalsIgnoreCase("on");
        } else {
          this.timing = !this.timing;
        }
        println("timing: " + (this.timing ? "on" : "off"));
        return true;
      case "\\set":
        if (parts.length >= 3 && parts[1].equals("format")) {
          try {
            this.format = Main.Format.valueOf(parts[2]);
            println("format: " + this.format);
          } catch (IllegalArgumentException iae) {
            println(Color.red("Unknown format: ") + parts[2]);
          }
          return true;
        }
        return false;
      case "\\connect":
        if (parts.length >= 2) {
          tryConnect(parts[1]);
        } else {
          println("usage: \\connect <jdbc-url>");
        }
        return true;
      case "\\import":
        if (parts.length >= 4) {
          ensureConn();
          String fmt = parts[1];
          File file = new File(parts[2]);
          String table = parts[3];
          int n;
          if (fmt.equalsIgnoreCase("csv")) {
            n = CsvIO.importCsv(conn, table, file.toPath(), 500);
          } else if (fmt.equalsIgnoreCase("json")) {
            n = JsonIO.importJson(conn, table, file.toPath(), 500);
          } else {
            println(Color.red("Unsupported import format: ") + fmt);
            return true;
          }
          conn.commit();
          println(Color.green("Imported ") + n + " rows into " + table);
        } else {
          println("usage: \\import csv|json <file> <table>");
        }
        return true;
      case "\\export":
        if (parts.length >= 4) {
          ensureConn();
          String fmt = parts[1];
          String table = parts[2];
          File file = new File(parts[3]);
          try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT * FROM " + table)) {
            int n;
            if (fmt.equalsIgnoreCase("csv")) {
              n = CsvIO.exportCsv(rs, file.toPath());
            } else if (fmt.equalsIgnoreCase("json")) {
              n = JsonIO.exportJson(rs, file.toPath());
            } else {
              println(Color.red("Unsupported export format: ") + fmt);
              return true;
            }
            println(Color.green("Exported ") + n + " rows from " + table + " -> " + file);
          }
        } else {
          println("usage: \\export csv|json <table> <file>");
        }
        return true;
      default:
        return false;
    }
  }

  private void execute(String sql) {
    try {
      ensureConn();
      try (Statement st = conn.createStatement()) {
        long start = System.nanoTime();
        boolean hasRs = isQuery(sql);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        if (hasRs) {
          try (ResultSet rs = st.executeQuery(sql)) {
            switch (format) {
              case table:
                System.out.print(TableRenderer.render(rs));
                break;
              case csv:
                System.out.print(renderCsv(rs));
                break;
              case json:
                System.out.print(renderJson(rs));
                break;
            }
          }
        } else {
          int uc = 0; try { uc = st.executeUpdate(sql); } catch (SQLException ignore) {}
          println(Color.green("OK") + " (" + uc + " rows affected)");
        }
        if (timing) println(Color.dim("Time: " + elapsedMs + " ms"));
      }
    } catch (SQLException se) {
      println(Color.red("SQL ERROR: ") + se.getMessage());
    } catch (Exception e) {
      println(Color.red("ERROR: ") + e.getMessage());
    }
  }

  private String renderCsv(ResultSet rs) throws Exception {
    // build a CSV string (including header)
    java.io.StringWriter sw = new StringWriter();
    com.opencsv.CSVWriter w = new com.opencsv.CSVWriter(sw);
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();
    String[] header = new String[cols];
    for (int i = 1; i <= cols; i++) header[i-1] = md.getColumnLabel(i);
    w.writeNext(header, false);
    while (rs.next()) {
      String[] row = new String[cols];
      for (int i = 1; i <= cols; i++) row[i-1] = rs.getString(i);
      w.writeNext(row, false);
    }
    w.flush();
    return sw.toString();
  }

  private String renderJson(ResultSet rs) throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper M = new com.fasterxml.jackson.databind.ObjectMapper();
    List<java.util.Map<String, Object>> list = new ArrayList<>();
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();
    while (rs.next()) {
      java.util.Map<String, Object> obj = new java.util.LinkedHashMap<>();
      for (int i = 1; i <= cols; i++) {
        String name = md.getColumnLabel(i);
        Object v = rs.getObject(i);
        obj.put(name, v);
      }
      list.add(obj);
    }
    return M.writerWithDefaultPrettyPrinter().writeValueAsString(list) + System.lineSeparator();
  }

  private void ensureConn() throws Exception {
    if (this.conn == null || this.conn.isClosed()) {
      try { tryConnect(resolveUrl()); } catch (Exception ce) { println(com.dafei1288.jimsql.cli.render.Color.yellow("Not connected. Type \\connect <jdbc-url> or set JIMSQL_URL (tip: add ?protocol=legacy|jspv1)")); }
    }
    if (this.conn == null || this.conn.isClosed()) {
      throw new IllegalStateException("Not connected. Use \\connect <jdbc-url>.");
    }
  }

  private void tryConnect(String u) throws Exception {
    if (u == null || u.isEmpty()) return;
    java.util.Properties props = new java.util.Properties();
    if (user != null && !user.isEmpty()) props.setProperty("user", user);
    if (password != null && !password.isEmpty()) props.setProperty("password", password);
    if (this.conn != null) try { this.conn.close(); } catch (Exception ignore) {}
    this.conn = props.isEmpty() ? DriverManager.getConnection(u) : DriverManager.getConnection(u, props);
    this.conn.setAutoCommit(false);
    println(Color.green("Connected: ") + u);
  }

  private String resolveUrl() {
    if (url != null && !url.isEmpty()) return url;
    String envUrl = System.getenv("JIMSQL_URL");
    if (envUrl != null && !envUrl.isEmpty()) return envUrl;
    String h = host != null ? host : firstNonNull(System.getenv("JIMSQL_HOST"), "127.0.0.1");
    String p = port != null ? String.valueOf(port) : firstNonNull(System.getenv("JIMSQL_PORT"), "8821");
    String d = db != null ? db : System.getenv("JIMSQL_DB");
    if (d == null || d.isEmpty()) return null;
    return "jdbc:jimsql://" + h + ":" + p + "/" + d;
  }

  private static String firstNonNull(String... vals) { for (String v : vals) if (v != null && !v.isEmpty()) return v; return null; }

  private static boolean isComplete(StringBuilder sb) {
    boolean inS = false, inD = false;
    for (int i = 0; i < sb.length(); i++) {
      char c = sb.charAt(i);
      if (c == '\'' && !inD) inS = !inS;
      else if (c == '"' && !inS) inD = !inD;
      else if (c == ';' && !inS && !inD) return true;
    }
    return false;
  }
  private static boolean isQuery(String s) {
    String t = s.trim().toLowerCase(java.util.Locale.ROOT);
    return t.startsWith("select") || t.startsWith("show") || t.startsWith("describe") || t.startsWith("explain");
  }
  private static void println(String s) { System.out.println(s); }
}


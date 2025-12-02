package com.dafei1288.jimsql.cli;

import com.dafei1288.jimsql.cli.io.CsvIO;
import com.dafei1288.jimsql.cli.io.JsonIO;
import com.dafei1288.jimsql.cli.render.TableRenderer;
import com.dafei1288.jimsql.cli.render.Color;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;

@Command(name = "jimsql", mixinStandardHelpOptions = true,
        description = "JimSQL CLI: query runner and CSV/JSON import/export.")
public class Main implements Callable<Integer> {

  enum Format { table, csv, json }
  enum ColorMode { auto, always, never }
  enum IoFmt { csv, json }

  @Option(names = "--url", description = "JDBC URL, e.g. jdbc:jimsql://host:port/db")
  String url;

  @Option(names = {"-H", "--host"}, description = "Host, default 127.0.0.1")
  String host;

  @Option(names = {"-p", "--port"}, description = "Port, default 8821")
  Integer port;

  @Option(names = {"-d", "--db"}, description = "Database name")
  String db;

  @Option(names = {"-u", "--user"}, description = "User")
  String user;

  @Option(names = {"-P", "--password"}, description = "Password", interactive = true, arity = "0..1")
  String password;

  @Option(names = {"-c", "--command"}, description = "Execute SQL string")
  String sql;

  @Option(names = {"-f", "--file"}, description = "Execute SQL from file")
  File sqlFile;

  @Option(names = "--format", description = "Output format: ${COMPLETION-CANDIDATES}", defaultValue = "table")
  Format format = Format.table;

  @Option(names = "--color", description = "Color mode: ${COMPLETION-CANDIDATES}", defaultValue = "always")
  ColorMode colorMode = ColorMode.always;

  // Import / Export
  @Option(names = "--import", description = "Import data format: ${COMPLETION-CANDIDATES}")
  IoFmt importFmt;

  @Option(names = "--export", description = "Export data format: ${COMPLETION-CANDIDATES}")
  IoFmt exportFmt;

  @Option(names = "--into", description = "Target table for import")
  String intoTable;

  @Option(names = "--table", description = "Source table for export")
  String fromTable;

  @Option(names = "--query", description = "Query for export")
  String exportQuery;

  @Option(names = {"-o", "--out"}, description = "Output file (export)")
  File outFile;

  @Option(names = {"-i", "--in"}, description = "Input file (import)")
  File inFile;

  public static void main(String[] args) {
    try { org.fusesource.jansi.AnsiConsole.systemInstall(); } catch (Throwable ignore) {}
    int code = new CommandLine(new Main()).execute(args);
    try { org.fusesource.jansi.AnsiConsole.systemUninstall(); } catch (Throwable ignore) {}
    System.exit(code);
  }

  @Override
  public Integer call() {
    Color.setMode(Color.toMode(colorMode));
    // Handle import/export first
    if (importFmt != null && exportFmt != null) {
      err("Cannot use --import and --export together");
      return 2;
    }
    try {
      if (importFmt != null) {
        return doImport();
      }
      if (exportFmt != null) {
        return doExport();
      }
      return doExecute();
    } catch (Exception e) {
      err(suggest(e));
      return 1;
    }
  }

  private int doImport() throws Exception {
    if (inFile == null) {
      throw new IllegalArgumentException("--in <file> is required for import");
    }
    if (intoTable == null || intoTable.isEmpty()) {
      throw new IllegalArgumentException("--into <table> is required for import");
    }
    try (Connection conn = connect()) {
      conn.setAutoCommit(false);
      int count;
      switch (importFmt) {
        case csv:
          count = CsvIO.importCsv(conn, intoTable, inFile.toPath(), 500);
          break;
        case json:
          count = JsonIO.importJson(conn, intoTable, inFile.toPath(), 500);
          break;
        default:
          throw new IllegalArgumentException("Unsupported import format: " + importFmt);
      }
      conn.commit();
      out("Imported " + count + " rows into " + intoTable);
      return 0;
    }
  }

  private int doExport() throws Exception {
    if (outFile == null) {
      throw new IllegalArgumentException("--out <file> is required for export");
    }
    String q = exportQuery;
    if (q == null || q.isEmpty()) {
      if (fromTable == null || fromTable.isEmpty()) {
        throw new IllegalArgumentException("Use --query or --table for export");
      }
      q = "SELECT * FROM " + fromTable;
    }
    try (Connection conn = connect(); Statement st = conn.createStatement()) {
      try (ResultSet rs = st.executeQuery(q)) {
        int count;
        switch (exportFmt) {
          case csv:
            count = CsvIO.exportCsv(rs, outFile.toPath());
            break;
          case json:
            count = JsonIO.exportJson(rs, outFile.toPath());
            break;
          default:
            throw new IllegalArgumentException("Unsupported export format: " + exportFmt);
        }
        out("Exported " + count + " rows from query");
        return 0;
      }
    }
  }

  private int doExecute() throws Exception {
    String text = this.sql;
    if (text == null && sqlFile != null) {
      text = Files.readString(sqlFile.toPath(), StandardCharsets.UTF_8);
    }
    if (text == null) {
      // try read from stdin if piped
      if (System.console() == null) {
        text = new String(System.in.readAllBytes(), StandardCharsets.UTF_8);
      }
    }
    if (text == null || text.trim().isEmpty()) {
      // Start REPL when no SQL provided
      return new Repl(url, host, port, db, user, password, format).run();
    }

    try (Connection conn = connect(); Statement st = conn.createStatement()) {
      List<String> stmts = splitSql(text);
      int lastUpdateCount = -1;
      for (String s : stmts) {
        String trimmed = s.trim();
        if (trimmed.isEmpty()) continue;
        long start = System.nanoTime();
        boolean hasRs = isQuery(trimmed);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        if (hasRs) {
          try (ResultSet rs = st.executeQuery(trimmed)) {
            String outStr = TableRenderer.render(rs);
            System.out.print(outStr);
          }
        } else {
          try { lastUpdateCount = st.executeUpdate(trimmed); } catch (SQLException ignore) { lastUpdateCount = 0; }
          out(Color.green("OK" ) + " (" + lastUpdateCount + " rows affected)" );
        }
        out(Color.dim("Time: " + elapsedMs + " ms"));
      }
      return 0;
    }
  }

  private Connection connect() throws Exception {
    Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
    String u = resolveUrl();
    if (u == null || u.isEmpty()) {
      throw new IllegalArgumentException("Connection URL is required (use --url or env JIMSQL_URL or host/port/db)");
    }
    Properties props = new Properties();
    if (user != null) props.setProperty("user", user);
    if (password != null) props.setProperty("password", password);
    if (props.isEmpty()) {
      return DriverManager.getConnection(u);
    } else {
      return DriverManager.getConnection(u, props);
    }
  }

  private String resolveUrl() {
    if (url != null && !url.isEmpty()) return url;
    String envUrl = System.getenv("JIMSQL_URL");
    if (envUrl != null && !envUrl.isEmpty()) return envUrl;
    String h = firstNonNull(host, System.getenv("JIMSQL_HOST"), "127.0.0.1");
    String p = firstNonNull(port != null ? String.valueOf(port) : null, System.getenv("JIMSQL_PORT"), "8821");
    String d = firstNonNull(db, System.getenv("JIMSQL_DB"), null);
    if (d == null) return null;
    return "jdbc:jimsql://" + h + ":" + p + "/" + d;
  }

  private static String firstNonNull(String... vals) {
    for (String v : vals) if (v != null && !v.isEmpty()) return v; return null;
  }

  private static List<String> splitSql(String sql) {
    // naive splitter: split by ';' not inside quotes
    List<String> parts = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    boolean inSingle = false, inDouble = false;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '\'' && !inDouble) inSingle = !inSingle;
      if (c == '"' && !inSingle) inDouble = !inDouble;
      if (c == ';' && !inSingle && !inDouble) {
        parts.add(sb.toString());
        sb.setLength(0);
      } else {
        sb.append(c);
      }
    }
    if (sb.length() > 0) parts.add(sb.toString());
    return parts;
  }

  private static boolean isQuery(String s) {
    String t = s.trim().toLowerCase(java.util.Locale.ROOT);
    return t.startsWith("select") || t.startsWith("show") || t.startsWith("describe") || t.startsWith("explain");
  }

  private static void out(String s) { System.out.println(s); }
  private static void err(String s) { System.err.println(Color.red("ERROR: ") + s); }
  private static String suggest(Exception e) {
    String m = String.valueOf(e.getMessage());
    String tip = "";
    String lower = m != null ? m.toLowerCase(java.util.Locale.ROOT) : "";
    if (lower.contains("bad magic") || lower.contains("protocol") || lower.contains("stream") || lower.contains("socket")) {
      tip = " Hint: try appending ?protocol=legacy or ?protocol=jspv1 to your JDBC URL.";
    }
    return m + tip;
  }
}
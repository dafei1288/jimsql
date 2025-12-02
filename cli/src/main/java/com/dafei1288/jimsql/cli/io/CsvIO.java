package com.dafei1288.jimsql.cli.io;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.*;

public final class CsvIO {
  private CsvIO() {}

  public static int importCsv(Connection conn, String table, Path path, int batchSize) throws Exception {
    try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(new FileInputStream(path.toFile()), StandardCharsets.UTF_8)).build()) {
      String[] header = reader.readNext();
      if (header == null) throw new IllegalArgumentException("CSV is empty");
      String placeholders = String.join(",", java.util.Collections.nCopies(header.length, "?"));
      String cols = String.join(",", header);
      String sql = "INSERT INTO " + table + " (" + cols + ") VALUES (" + placeholders + ")";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int count = 0; int bat = 0; String[] row;
        while ((row = reader.readNext()) != null) {
          for (int i = 0; i < header.length; i++) {
            String v = i < row.length ? row[i] : null;
            if (v == null || v.isEmpty()) ps.setNull(i+1, Types.VARCHAR); else ps.setString(i+1, v);
          }
          ps.addBatch(); bat++; count++;
          if (bat >= batchSize) { ps.executeBatch(); bat = 0; }
        }
        if (bat > 0) ps.executeBatch();
        return count;
      }
    }
  }

  public static int exportCsv(ResultSet rs, Path path) throws SQLException, IOException {
    try (OutputStreamWriter w = new OutputStreamWriter(new FileOutputStream(path.toFile()), StandardCharsets.UTF_8);
         CSVWriter writer = new CSVWriter(w)) {
      ResultSetMetaData md = rs.getMetaData();
      int cols = md.getColumnCount();
      String[] header = new String[cols];
      for (int i = 1; i <= cols; i++) header[i-1] = md.getColumnLabel(i);
      writer.writeNext(header, false);
      int count = 0;
      while (rs.next()) {
        String[] row = new String[cols];
        for (int i = 1; i <= cols; i++) {
          String v = rs.getString(i);
          row[i-1] = v;
        }
        writer.writeNext(row, false);
        count++;
      }
      return count;
    }
  }
}




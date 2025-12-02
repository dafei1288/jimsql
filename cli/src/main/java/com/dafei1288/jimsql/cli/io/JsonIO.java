package com.dafei1288.jimsql.cli.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class JsonIO {
  private static final ObjectMapper M = new ObjectMapper();
  private JsonIO() {}

  public static int importJson(Connection conn, String table, Path path, int batchSize) throws Exception {
    JsonNode root = M.readTree(new File(path.toString()));
    if (!root.isArray() || root.size() == 0) throw new IllegalArgumentException("JSON must be a non-empty array of objects");
    JsonNode first = root.get(0);
    if (!first.isObject()) throw new IllegalArgumentException("JSON array elements must be objects");
    // Build column list from first object keys (stable order)
    Iterator<String> it = first.fieldNames();
    java.util.List<String> cols = new java.util.ArrayList<>();
    while (it.hasNext()) cols.add(it.next());
    String placeholders = String.join(",", java.util.Collections.nCopies(cols.size(), "?"));
    String colList = String.join(",", cols);
    String sql = "INSERT INTO " + table + " (" + colList + ") VALUES (" + placeholders + ")";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      int count = 0; int bat = 0;
      for (JsonNode node : root) {
        for (int i = 0; i < cols.size(); i++) {
          String key = cols.get(i);
          JsonNode v = node.get(key);
          if (v == null || v.isNull()) ps.setNull(i+1, Types.VARCHAR);
          else ps.setString(i+1, v.asText());
        }
        ps.addBatch(); bat++; count++;
        if (bat >= batchSize) { ps.executeBatch(); bat = 0; }
      }
      if (bat > 0) ps.executeBatch();
      return count;
    }
  }

  public static int exportJson(ResultSet rs, Path path) throws SQLException, IOException {
    JsonFactory f = new JsonFactory();
    try (FileOutputStream fos = new FileOutputStream(path.toFile());
         JsonGenerator g = f.createGenerator(fos)) {
      g.setCodec(M);
      g.writeStartArray();
      ResultSetMetaData md = rs.getMetaData();
      int cols = md.getColumnCount();
      int count = 0;
      while (rs.next()) {
        g.writeStartObject();
        for (int i = 1; i <= cols; i++) {
          String name = md.getColumnLabel(i);
          String v = rs.getString(i);
          if (v == null) g.writeNullField(name); else g.writeStringField(name, v);
        }
        g.writeEndObject();
        count++;
      }
      g.writeEndArray();
      g.flush();
      return count;
    }
  }
}

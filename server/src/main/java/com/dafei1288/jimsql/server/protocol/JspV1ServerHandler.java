package com.dafei1288.jimsql.server.protocol;

import com.dafei1288.jimsql.common.JqQueryReq;
import com.dafei1288.jimsql.common.JqResultSetMetaData;
import com.dafei1288.jimsql.common.protocol.FrameHeader;
import com.dafei1288.jimsql.common.protocol.MessageType;
import com.dafei1288.jimsql.common.protocol.ProtocolFrame;
import com.dafei1288.jimsql.common.protocol.RowBinary;
import com.dafei1288.jimsql.common.protocol.Compression;
// import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
// import com.dafei1288.jimsql.server.parser.SqlParser;
// import com.dafei1288.jimsql.server.parser.dql.SelectTableParseTreeProcessor;
// import com.dafei1288.jimsql.server.plan.logical.OptimizeQueryLogicalPlan;
// import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import com.dafei1288.jimsql.server.instance.ServerMetadata;
import com.dafei1288.jimsql.common.JqColumnResultSetMetadata;
import com.dafei1288.jimsql.common.meta.JqTable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JspV1ServerHandler extends SimpleChannelInboundHandler<ProtocolFrame> {

  private static final AttributeKey<String> ATTR_DB = AttributeKey.valueOf("jspv1.db");
  private static final AttributeKey<Boolean> ATTR_COMPRESS = AttributeKey.valueOf("jspv1.compress");
  private static final AttributeKey<java.util.Map<String, CursorState>> ATTR_CURSORS = AttributeKey.valueOf("jspv1.cursors");
  private static final AttributeKey<Integer> ATTR_CURSOR_SEQ = AttributeKey.valueOf("jspv1.cursor.seq");
  private static final String WIRELOG = System.getProperty("jimsql.wirelog", System.getenv("JIMSQL_WIRELOG"));

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ProtocolFrame msg) throws Exception {
    if ("json".equalsIgnoreCase(WIRELOG)) {
      int typeCode = msg.header.type & 0xFF;
      String kind = MessageType.fromCode(typeCode).name();
      String extra = (typeCode == MessageType.RESULTSET_BATCH.code) ? ("len="+msg.header.payloadLen+", flags="+msg.header.flags) : new String(msg.payload, StandardCharsets.UTF_8);
      System.out.printf("[wirelog] S<-C %s req=%d flags=%d len=%d payload=%s%n", kind, msg.header.requestId, (int)msg.header.flags, msg.header.payloadLen, extra);
    }
    MessageType type = MessageType.fromCode(msg.header.type & 0xFF);
    switch (type) {
      case HELLO -> handleHello(ctx, msg);
      case QUERY -> handleQuery(ctx, msg);
      case CURSOR_FETCH -> handleFetch(ctx, msg);
      case CURSOR_CLOSE -> handleClose(ctx, msg);
      case PING -> sendSimple(ctx, MessageType.PONG, msg.header.requestId, "{}");
      case CANCEL -> sendError(ctx, msg.header.requestId, 1003, "57014", "Cancelled");
      default -> sendError(ctx, msg.header.requestId, 1999, "HY000", "Unsupported message: "+type);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof java.net.SocketException) {
      String msg = cause.getMessage();
      if (msg != null && msg.toLowerCase().contains("connection reset")) {
        // Client closed abruptly; treat as normal and just close the channel.
        ctx.close();
        return;
      }
    }
    // For other exceptions, respond with ERROR once if possible, then close.
    try {
      sendError(ctx, 0L, 1999, "HY000", cause.getMessage());
    } catch (Exception ignored) {}
    ctx.close();
  }

  private void handleHello(ChannelHandlerContext ctx, ProtocolFrame msg) {
    String body = new String(msg.payload, StandardCharsets.UTF_8);
    String db = extractJsonString(body, "db");
    if (db != null) {
      ctx.channel().attr(ATTR_DB).set(db);
    }
    boolean wantLz4 = body.contains("\"lz4\"");
    ctx.channel().attr(ATTR_COMPRESS).set(wantLz4);
    ctx.channel().attr(ATTR_CURSORS).set(new java.util.HashMap<>());
    ctx.channel().attr(ATTR_CURSOR_SEQ).set(1);
    String ack = "{\"server\":\"jimsql/1.0\",\"cap\":{\"compress\":\"lz4\",\"row\":\"row_bin_v1\",\"cursor\":true},\"auth\":\"none\"}";
    sendSimple(ctx, MessageType.HELLO_ACK, msg.header.requestId, ack);
  }

  private void handleQuery(ChannelHandlerContext ctx, ProtocolFrame msg) {
    try {
      String payload = new String(msg.payload, StandardCharsets.UTF_8);
      String sql = extractJsonString(payload, "sql");
      boolean openCursor = payload.contains("\"openCursor\":true");
      String db = ctx.channel().attr(ATTR_DB).get();
      if (db == null) db = "test";

      // Parse SQL (simple SELECT ... FROM ... parser to avoid runtime javac flags)
      ParsedSelect parsed = parseSelect(sql);
      if (parsed == null) {
        sendError(ctx, msg.header.requestId, 1001, "42000", "Only simple SELECT is supported in JSPv1 path");
        return;
      }
      String tableName = parsed.table;
      List<String> selectedCols = parsed.columns; // null => '*'
      JqTable jt = ServerMetadata.getInstance().fetchTableByName(db, tableName);
      if (jt == null) { sendError(ctx, msg.header.requestId, 1001, "42P01", "table not found: "+tableName); return; }
      java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> all = jt.getJqTableLinkedHashMap();
      List<String> projectCols = (selectedCols == null || selectedCols.isEmpty()) ? new java.util.ArrayList<>(all.keySet()) : selectedCols;

      // Build metadata
      LinkedHashMap<String, JqColumnResultSetMetadata> cmap = new LinkedHashMap<>();
      for (int i = 0; i < projectCols.size(); i++) {
        String col = projectCols.get(i);
        com.dafei1288.jimsql.common.meta.JqColumn jc = all.get(col);
        if (jc == null) continue;
        JqColumnResultSetMetadata m = new JqColumnResultSetMetadata();
        m.setIndex(i+1);
        m.setLabelName(col);
        m.setTableName(tableName);
        m.setColumnType(jc.getColumnType());
        m.setClazz(jc.getColumnClazzType());
        m.setClazzStr(jc.getColumnClazzType().getName());
        cmap.put(col, m);
      }
      JqResultSetMetaData meta = new JqResultSetMetaData(cmap);

      // Build HEADER JSON
      cmap = meta.getColumnMeta();
      List<Map<String,Object>> cols = new ArrayList<>();
      for (Map.Entry<String, JqColumnResultSetMetadata> e : cmap.entrySet()) {
        JqColumnResultSetMetadata m = e.getValue();
        cols.add(Map.of(
            "name", e.getKey(),
            "label", m.getLabelName(),
            "type", m.getColumnType(),
            "table", m.getTableName()==null?"":m.getTableName()
        ));
      }
      String cursorId = null;
      if (openCursor) {
        Integer seqObj = ctx.channel().attr(ATTR_CURSOR_SEQ).get();
        int seq = (seqObj == null) ? 1 : seqObj;
        cursorId = "c" + seq;
        ctx.channel().attr(ATTR_CURSOR_SEQ).set(seq + 1);
      }
      String headerJson = jsonOfHeader(cols, cursorId);
      sendSimple(ctx, MessageType.RESULTSET_HEADER, msg.header.requestId, headerJson);

      // Stream data batches (simple CSV reader, aligned with existing QueryPhysicalPlan)
      java.util.List<String> lines = java.nio.file.Files.readAllLines(jt.getBasepath().toPath());
      // Determine projected columns order
      // projectCols already determined

      // Build header index
      java.util.List<String> header = new java.util.ArrayList<>(jt.getJqTableLinkedHashMap().keySet());
      java.util.Map<String,Integer> idx = new java.util.HashMap<>();
      for (int c = 0; c < header.size(); c++) idx.put(header.get(c), c);

      // types array from metadata (avoid lambda to keep locals effectively final)
      int[] types = new int[projectCols.size()];
      for (int i = 0; i < projectCols.size(); i++) {
        String k = projectCols.get(i);
        JqColumnResultSetMetadata m = cmap.get(k);
        types[i] = (m != null && m.getColumnType() != null) ? m.getColumnType() : java.sql.Types.VARCHAR;
      }

      if (openCursor) {
        CursorState st = new CursorState(projectCols, types, lines, idx, 1);
        java.util.Map<String, CursorState> map = ctx.channel().attr(ATTR_CURSORS).get();
        if (map == null) { map = new java.util.HashMap<>(); ctx.channel().attr(ATTR_CURSORS).set(map); }
        map.put(cursorId, st);
        if (st.pos >= st.lines.size()) {
          sendSimple(ctx, MessageType.RESULTSET_END, msg.header.requestId, String.format("{\"rows\":%d,\"warnings\":[]}", st.rowsSent));
          map.remove(cursorId);
        }
      } else {
        int batchSize = 512;
        int rowCount = 0;
        List<String[]> batch = new ArrayList<>(batchSize);
        for (int i = 1; i < lines.size(); i++) { // skip header
          String[] fields = lines.get(i).split(com.dafei1288.jimsql.common.Utils.COLUMN_SPILTOR, -1);
          String[] outRow = new String[projectCols.size()];
          for (int c = 0; c < projectCols.size(); c++) {
            Integer pos = idx.get(projectCols.get(c));
            outRow[c] = (pos != null && pos < fields.length) ? fields[pos] : null;
          }
          batch.add(outRow);
          rowCount++;
          if (batch.size() >= batchSize) { sendTypedBatch(ctx, msg.header.requestId, batch, types); batch.clear(); }
        }
        if (!batch.isEmpty()) { sendTypedBatch(ctx, msg.header.requestId, batch, types); }
        String endJson = String.format("{\"rows\":%d,\"warnings\":[]}", rowCount);
        sendSimple(ctx, MessageType.RESULTSET_END, msg.header.requestId, endJson);
      }
    } catch (Exception ex) {
      sendError(ctx, msg.header.requestId, 1999, "HY000", ex.getMessage()==null?"internal":ex.getMessage());
    }
  }

  private void sendTypedBatch(ChannelHandlerContext ctx, long requestId, List<String[]> rows, int[] types) throws java.io.IOException {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    RowBinary.writeTypedBatch(baos, rows, types.length, types);
    byte[] payload = baos.toByteArray();
    short flags = 0;
    Boolean comp = ctx.channel().attr(ATTR_COMPRESS).get();
    if (comp != null && comp) { payload = Compression.lz4(payload); flags |= 0x1; }
    FrameHeader h = new FrameHeader((byte)1, (byte)MessageType.RESULTSET_BATCH.code, flags, requestId, payload.length, 0);
    if ("json".equalsIgnoreCase(WIRELOG)) {
      System.out.printf("[wirelog] S->C RESULTSET_BATCH req=%d flags=%d len=%d rows=%d%n", requestId, (int)flags, payload.length, rows.size());
    }
    ctx.writeAndFlush(new ProtocolFrame(h, payload));
  }

  private void sendSimple(ChannelHandlerContext ctx, MessageType type, long requestId, String json) {
    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    FrameHeader h = new FrameHeader((byte)1, (byte)type.code, (short)0, requestId, bytes.length, 0);
    if ("json".equalsIgnoreCase(WIRELOG)) {
      System.out.printf("[wirelog] S->C %s req=%d flags=%d len=%d payload=%s%n", type.name(), requestId, 0, bytes.length, json);
    }
    ctx.writeAndFlush(new ProtocolFrame(h, bytes));
  }

  private void sendError(ChannelHandlerContext ctx, long requestId, int code, String sqlState, String message) {
    String json = String.format("{\"code\":%d,\"sqlState\":\"%s\",\"message\":\"%s\"}", code, sqlState, message==null?"":message.replace("\"","'"));
    sendSimple(ctx, MessageType.ERROR, requestId, json);
  }

  private static String jsonOfHeader(List<Map<String,Object>> cols, String cursorId) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"cols\":[");
    for (int i = 0; i < cols.size(); i++) {
      Map<String,Object> c = cols.get(i);
      if (i>0) sb.append(',');
      sb.append('{')
        .append("\"name\":\"").append(escape((String)c.get("name"))).append("\",")
        .append("\"label\":\"").append(escape((String)c.get("label"))).append("\",")
        .append("\"type\":").append(c.get("type").toString()).append(',')
        .append("\"table\":\"").append(escape((String)c.get("table"))).append("\"")
        .append('}');
    }
    sb.append(']');
    if (cursorId != null) {
      sb.append(",\"cursorId\":\"").append(escape(cursorId)).append("\"");
    }
    sb.append('}');
    return sb.toString();
  }

  private static String extractJsonString(String json, String key) {
    String pat = "\"" + key + "\":\"";
    int i = json.indexOf(pat);
    if (i < 0) return null;
    int s = i + pat.length();
    StringBuilder sb = new StringBuilder();
    boolean esc = false;
    for (int p = s; p < json.length(); p++) {
      char ch = json.charAt(p);
      if (esc) { sb.append(ch); esc = false; continue; }
      if (ch == '\\') { esc = true; continue; }
      if (ch == '"') break;
      sb.append(ch);
    }
    return sb.toString();
  }

  private static int extractInt(String json, String key, int defVal) {
    String pat = "\"" + key + "\":";
    int i = json.indexOf(pat);
    if (i < 0) return defVal;
    int s = i + pat.length();
    int e = s;
    while (e < json.length() && (Character.isDigit(json.charAt(e)))) e++;
    try { return Integer.parseInt(json.substring(s, e)); } catch (Exception ex) { return defVal; }
  }

  private void handleFetch(ChannelHandlerContext ctx, ProtocolFrame msg) {
    String body = new String(msg.payload, StandardCharsets.UTF_8);
    String cursorId = extractJsonString(body, "cursorId");
    int fetchSize = extractInt(body, "fetchSize", 500);
    java.util.Map<String, CursorState> map = ctx.channel().attr(ATTR_CURSORS).get();
    if (map == null || cursorId == null) { sendError(ctx, msg.header.requestId, 1999, "HY000", "no cursor"); return; }
    CursorState st = map.get(cursorId);
    if (st == null) { sendError(ctx, msg.header.requestId, 1999, "HY000", "invalid cursor"); return; }
    try {
      sendNextBatch(ctx, msg.header.requestId, st, fetchSize);
      if (st.pos >= st.lines.size()) {
        sendSimple(ctx, MessageType.RESULTSET_END, msg.header.requestId, String.format("{\"rows\":%d,\"warnings\":[]}", st.rowsSent));
        map.remove(cursorId);
      }
    } catch (Exception ex) {
      sendError(ctx, msg.header.requestId, 1999, "HY000", ex.getMessage());
    }
  }

  private void handleClose(ChannelHandlerContext ctx, ProtocolFrame msg) {
    String body = new String(msg.payload, StandardCharsets.UTF_8);
    String cursorId = extractJsonString(body, "cursorId");
    java.util.Map<String, CursorState> map = ctx.channel().attr(ATTR_CURSORS).get();
    if (map != null && cursorId != null) map.remove(cursorId);
    sendSimple(ctx, MessageType.OK, msg.header.requestId, "{}");
  }

  private void sendNextBatch(ChannelHandlerContext ctx, long requestId, CursorState st, int fetchSize) throws java.io.IOException {
    java.util.List<String[]> batch = new java.util.ArrayList<>(fetchSize);
    for (int n=0; n<fetchSize && st.pos < st.lines.size(); n++) {
      String[] fields = st.lines.get(st.pos++).split(com.dafei1288.jimsql.common.Utils.COLUMN_SPILTOR, -1);
      String[] outRow = new String[st.projectCols.size()];
      for (int c = 0; c < st.projectCols.size(); c++) {
        Integer pos = st.idx.get(st.projectCols.get(c));
        outRow[c] = (pos != null && pos < fields.length) ? fields[pos] : null;
      }
      batch.add(outRow);
      st.rowsSent++;
    }
    if (!batch.isEmpty()) sendTypedBatch(ctx, requestId, batch, st.types);
  }

  static class CursorState {
    final java.util.List<String> projectCols;
    final int[] types;
    final java.util.List<String> lines;
    final java.util.Map<String,Integer> idx;
    int pos;
    int rowsSent;
    CursorState(java.util.List<String> projectCols, int[] types, java.util.List<String> lines, java.util.Map<String,Integer> idx, int pos) {
      this.projectCols = projectCols; this.types = types; this.lines = lines; this.idx = idx; this.pos = pos; this.rowsSent=0;
    }
  }

  private static String escape(String s){
    if (s==null) return "";
    return s.replace("\\","\\\\").replace("\"","\\\"");
  }

  private static class ParsedSelect {
    final String table; final java.util.List<String> columns; // null => *
    ParsedSelect(String table, java.util.List<String> columns){ this.table = table; this.columns = columns; }
  }

  private static ParsedSelect parseSelect(String sql){
    if (sql == null) return null;
    String s = sql.trim();
    String lower = s.toLowerCase();
    if (!lower.startsWith("select ")) return null;
    int fromIdx = lower.indexOf(" from ");
    if (fromIdx < 0) return null;
    String cols = s.substring(7, fromIdx).trim();
    String rest = s.substring(fromIdx + 6).trim();
    String table = rest.split("\\s+")[0];
    if ("*".equals(cols)) return new ParsedSelect(table, null);
    String[] parts = cols.split(",");
    java.util.List<String> list = new java.util.ArrayList<>();
    for (String p : parts) list.add(p.trim());
    return new ParsedSelect(table, list);
  }
}

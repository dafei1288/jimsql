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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JspV1ServerHandler extends SimpleChannelInboundHandler<ProtocolFrame> {

  
  private static final Logger LOG = LoggerFactory.getLogger(JspV1ServerHandler.class);
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
      LOG.info("[wirelog] S<-C {} req={} flags={} len={} payload={}", kind, msg.header.requestId, (int)msg.header.flags, msg.header.payloadLen, extra);
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
      // DCL/SHOW via ANTLR (SHOW DATABASES/TABLES/DESCRIBE/SHOW CREATE TABLE)
      try {
        com.dafei1288.jimsql.server.parser.SqlParser p = com.dafei1288.jimsql.server.parser.SqlParser.getInstance();
        com.dafei1288.jimsql.common.JqQueryReq req = new com.dafei1288.jimsql.common.JqQueryReq();
        req.setDb(db); req.setSql(sql);
        com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor spp = p.parser(req);
        org.snt.inmemantlr.tree.ParseTreeProcessor sub = (org.snt.inmemantlr.tree.ParseTreeProcessor) spp.process();
        com.dafei1288.jimsql.server.parser.SqlStatementEnum kind = spp.getSqlStatementEnum();
        if (kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_DATABASES
         || kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_TABLES
         || kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_TABLEDESC
         || kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_CREATE_TABLE) {
          java.util.List<java.util.Map<String,Object>> cols = new java.util.ArrayList<>();
          int[] types;
          java.util.List<String[]> rows = new java.util.ArrayList<>();
          com.dafei1288.jimsql.server.instance.ServerMetadata SM = com.dafei1288.jimsql.server.instance.ServerMetadata.getInstance();
          if (kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_DATABASES) {
            cols.add(java.util.Map.of("name","database","label","database","type",java.sql.Types.VARCHAR,"table",""));
            types = new int[]{ java.sql.Types.VARCHAR };
            java.util.List<String> dbs = new java.util.ArrayList<>(SM.getJqDatabaseLinkedHashMap().keySet());
            for (String d : dbs) rows.add(new String[]{ d });
          } else if (kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_TABLES) {
            cols.add(java.util.Map.of("name","table","label","table","type",java.sql.Types.VARCHAR,"table",""));
            types = new int[]{ java.sql.Types.VARCHAR };
            com.dafei1288.jimsql.common.meta.JqDatabase jdb = SM.fetchDatabaseByName(db);
            java.util.List<String> tables = (jdb==null) ? java.util.Collections.emptyList() : new java.util.ArrayList<>(jdb.getJqTableListMap().keySet());
            for (String tname : tables) rows.add(new String[]{ tname });
          } else if (kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_TABLEDESC) {
            cols.add(java.util.Map.of("name","Field","label","Field","type",java.sql.Types.VARCHAR,"table",""));
            cols.add(java.util.Map.of("name","Type","label","Type","type",java.sql.Types.VARCHAR,"table",""));
            types = new int[]{ java.sql.Types.VARCHAR, java.sql.Types.VARCHAR };
            String tname = null;
            if (sub instanceof com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor) {
              Object r = ((com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor) sub).getResult();
              if (r instanceof com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor.DclRequest) {
                tname = ((com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor.DclRequest) r).tableName;
              }
            }
            com.dafei1288.jimsql.common.meta.JqTable jt = (tname==null)? null : SM.fetchTableByName(db, tname);
            java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> cmap = (jt==null) ? new java.util.LinkedHashMap<>() : jt.getJqTableLinkedHashMap();
            for (java.util.Map.Entry<String, com.dafei1288.jimsql.common.meta.JqColumn> e : cmap.entrySet()) {
              rows.add(new String[]{ e.getKey(), sqlTypeToName(e.getValue().getColumnType()) });
            }
          } else {
            cols.add(java.util.Map.of("name","Table","label","Table","type",java.sql.Types.VARCHAR,"table",""));
            cols.add(java.util.Map.of("name","Create Table","label","Create Table","type",java.sql.Types.VARCHAR,"table",""));
            types = new int[]{ java.sql.Types.VARCHAR, java.sql.Types.VARCHAR };
            String tname = null;
            if (sub instanceof com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor) {
              Object r = ((com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor) sub).getResult();
              if (r instanceof com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor.DclRequest) {
                tname = ((com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor.DclRequest) r).tableName;
              }
            }
            com.dafei1288.jimsql.common.meta.JqTable jt = (tname==null)? null : SM.fetchTableByName(db, tname);
            java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> cmap = (jt==null) ? new java.util.LinkedHashMap<>() : jt.getJqTableLinkedHashMap();
            String ddl = buildCreateTableDDL(tname, cmap);
            rows.add(new String[]{ tname, ddl });
          }
          String headerJson = jsonOfHeader(cols, null);
          sendSimple(ctx, com.dafei1288.jimsql.common.protocol.MessageType.RESULTSET_HEADER, msg.header.requestId, headerJson);
          if (!rows.isEmpty()) sendTypedBatch(ctx, msg.header.requestId, rows, types);
          String endJson = String.format("{\"rows\":%d,\"warnings\":[]}", rows.size());
          sendSimple(ctx, com.dafei1288.jimsql.common.protocol.MessageType.RESULTSET_END, msg.header.requestId, endJson);
          return;
        }
      } catch (Exception _ex_show) {
        // fall through to native handlers (SHOW TABLES etc.)
      }
      // DML (UPDATE/DELETE) path via ANTLR + CSV executor
      String sTrim = (sql == null) ? "" : sql.trim();
      String lowerTrim = sTrim.toLowerCase(java.util.Locale.ROOT);
      if (lowerTrim.startsWith("update") || lowerTrim.startsWith("delete")) {
        try {
          com.dafei1288.jimsql.server.parser.SqlParser p = com.dafei1288.jimsql.server.parser.SqlParser.getInstance();
          com.dafei1288.jimsql.common.JqQueryReq req = new com.dafei1288.jimsql.common.JqQueryReq();
          req.setDb(db); req.setSql(sql);
          com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor spp = p.parser(req);
          org.snt.inmemantlr.tree.ParseTreeProcessor sub = (org.snt.inmemantlr.tree.ParseTreeProcessor) spp.process();
          com.dafei1288.jimsql.server.parser.SqlStatementEnum kind = spp.getSqlStatementEnum();
          if (kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.UPDATE_TABLE) {
            Object r = ((com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor) sub).getResult();
            com.dafei1288.jimsql.server.plan.logical.UpdateLogicalPlan plan = (com.dafei1288.jimsql.server.plan.logical.UpdateLogicalPlan) r;
            int cnt = com.dafei1288.jimsql.server.plan.physical.DmlCsvExecutor.executeUpdate(db, plan);
            String payloadJson = String.format("{\"rows\":%d,\"warnings\":[]}", cnt);
            sendSimple(ctx, com.dafei1288.jimsql.common.protocol.MessageType.UPDATE_COUNT, msg.header.requestId, payloadJson);
            return;
          } else if (kind == com.dafei1288.jimsql.server.parser.SqlStatementEnum.DELETE_TABLE) {
            Object r = ((com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor) sub).getResult();
            com.dafei1288.jimsql.server.plan.logical.DeleteLogicalPlan plan = (com.dafei1288.jimsql.server.plan.logical.DeleteLogicalPlan) r;
            int cnt = com.dafei1288.jimsql.server.plan.physical.DmlCsvExecutor.executeDelete(db, plan);
            String payloadJson = String.format("{\"rows\":%d,\"warnings\":[]}", cnt);
            sendSimple(ctx, com.dafei1288.jimsql.common.protocol.MessageType.UPDATE_COUNT, msg.header.requestId, payloadJson);
            return;
          }
        } catch (Exception ex) {
          sendError(ctx, msg.header.requestId, 1999, "HY000", ex.getMessage()==null?"internal":ex.getMessage());
          return;
        }
      }

      ParsedSelect parsed = parseSelect(sql);
      if (parsed == null) {
        sendError(ctx, msg.header.requestId, 1001, "42000", "Only simple SELECT is supported in JSPv1 path");
        return;
      }
      String tableName = parsed.table;
      java.util.List<String> selectedCols = parsed.columns; // null => '*'
      com.dafei1288.jimsql.common.meta.JqTable jt = ServerMetadata.getInstance().fetchTableByName(db, tableName);
      if (jt == null) { sendError(ctx, msg.header.requestId, 1001, "42P01", "table not found: "+tableName); return; }
      java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> all = jt.getJqTableLinkedHashMap();
      java.util.List<String> projectCols = (selectedCols == null || selectedCols.isEmpty()) ? new java.util.ArrayList<>(all.keySet()) : selectedCols;

      ClauseParts parts = extractClauses(sql);

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

      cmap = meta.getColumnMeta();
      java.util.List<java.util.Map<String,Object>> cols = new java.util.ArrayList<>();
      for (java.util.Map.Entry<String, JqColumnResultSetMetadata> e : cmap.entrySet()) {
        JqColumnResultSetMetadata m = e.getValue();
        cols.add(java.util.Map.of(
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

      java.util.List<java.util.Map<String,String>> finalRows = buildFinalRows(jt, parts);

      int[] types = new int[projectCols.size()];
      for (int i = 0; i < projectCols.size(); i++) {
        String k = projectCols.get(i);
        JqColumnResultSetMetadata m = cmap.get(k);
        types[i] = (m != null && m.getColumnType() != null) ? m.getColumnType() : java.sql.Types.VARCHAR;
      }

      if (openCursor) {
        int fetchSize = extractInt(payload, "fetchSize", 500);
        java.util.List<String[]> batch = new java.util.ArrayList<>(fetchSize);
        int sent = 0;
        for (int r = 0; r < finalRows.size(); r++) {
          String[] outRow = new String[projectCols.size()];
          for (int c = 0; c < projectCols.size(); c++) outRow[c] = getCaseInsensitive(finalRows.get(r), projectCols.get(c));
          batch.add(outRow);
          if (batch.size() >= fetchSize) { sendTypedBatch(ctx, msg.header.requestId, batch, types); batch.clear(); }
          sent++;
        }
        if (!batch.isEmpty()) sendTypedBatch(ctx, msg.header.requestId, batch, types);
        String endJson = String.format("{\"rows\":%d,\"warnings\":[]}", sent);
        sendSimple(ctx, MessageType.RESULTSET_END, msg.header.requestId, endJson);
      } else {
        int batchSize = 512;
        java.util.List<String[]> batch = new java.util.ArrayList<>(batchSize);
        int rowCount = 0;
        for (int r = 0; r < finalRows.size(); r++) {
          String[] outRow = new String[projectCols.size()];
          for (int c = 0; c < projectCols.size(); c++) outRow[c] = getCaseInsensitive(finalRows.get(r), projectCols.get(c));
          batch.add(outRow); rowCount++;
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

  private static class ClauseParts { String where; java.util.List<OrderItem> order = new java.util.ArrayList<>(); Integer limit; Integer offset; }
  private static class OrderItem { String col; boolean asc = true; }

  private static ClauseParts extractClauses(String sql) {
    ClauseParts p = new ClauseParts();
    if (sql == null) return p;
    String raw = sql.trim();
    String sp = raw.replaceAll("\\s+", " ");
    String Usp = sp.toUpperCase(java.util.Locale.ROOT);
    String norm = raw.toUpperCase(java.util.Locale.ROOT).replaceAll("\\s+", "");
    int w = Usp.indexOf(" WHERE ");
    if (w >= 0) {
      int end = Usp.length();
      for (String kw : new String[]{" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT ", " OFFSET "}) {
        int k = Usp.indexOf(kw, w+1); if (k >= 0 && k < end) end = k;
      }
      if (end > w+7) p.where = sp.substring(w+7, end).trim();
    } else {
      int wn = norm.indexOf("WHERE");
      if (wn >= 0) {
        int endn = norm.length();
        for (String kw : new String[]{"GROUP","HAVING","ORDER","LIMIT","OFFSET"}) {
          int k = norm.indexOf(kw, wn+5); if (k >= 0 && k < endn) endn = k;
        }
        if (endn > wn+5) p.where = norm.substring(wn+5, endn);
      }
    }
    int ob = Usp.indexOf(" ORDER BY ");
    if (ob >= 0) {
      int endOb = Usp.length();
      for (String kw2 : new String[]{" LIMIT ", " HAVING ", " GROUP BY ", " OFFSET "}) {
        int k2 = Usp.indexOf(kw2, ob+1); if (k2 >= 0 && k2 < endOb) endOb = k2;
      }
      if (endOb > ob+10) {
        String ordSeg = sp.substring(ob+10, endOb).trim();
        String[] items = ordSeg.split(",");
        for (String it : items) {
          it = it.trim(); if (it.isEmpty()) continue;
          String[] toks = it.split("\\s+");
          OrderItem oi = new OrderItem();
          oi.col = stripQuotes(toks[0]); oi.asc = true;
          if (toks.length >= 2 && toks[1].equalsIgnoreCase("DESC")) oi.asc = false;
          p.order.add(oi);
        }
      }
    } else {
      int ob2 = norm.indexOf("ORDERBY");
      if (ob2 >= 0) {
        int end2 = norm.length();
        for (String kw : new String[]{"LIMIT","HAVING","GROUP","OFFSET"}) {
          int k = norm.indexOf(kw, ob2+7); if (k >= 0 && k < end2) end2 = k;
        }
        if (end2 > ob2+7) {
          String ord = norm.substring(ob2+7, end2);
          String[] parts = ord.split(",");
          for (String p2 : parts) {
            p2 = p2.trim(); if (p2.isEmpty()) continue;
            OrderItem oi = new OrderItem(); oi.asc = true; String col = p2;
            if (p2.endsWith("ASC")) { oi.asc = true; col = p2.substring(0, p2.length()-3); }
            else if (p2.endsWith("DESC")) { oi.asc = false; col = p2.substring(0, p2.length()-4); }
            oi.col = stripQuotes(col);
            p.order.add(oi);
          }
        }
      }
    }
    java.util.regex.Matcher m = java.util.regex.Pattern.compile("LIMIT([0-9]+)").matcher(norm);
    if (m.find()) { try { p.limit = Integer.parseInt(m.group(1)); } catch (Exception ignore) {} }
    java.util.regex.Matcher mo = java.util.regex.Pattern.compile("OFFSET([0-9]+)").matcher(norm);
    if (mo.find()) { try { p.offset = Integer.parseInt(mo.group(1)); } catch (Exception ignore) {} }
    return p;
  }

  private static java.util.List<java.util.Map<String,String>> buildFinalRows(com.dafei1288.jimsql.common.meta.JqTable jt, ClauseParts parts) throws java.io.IOException {
    java.util.List<String> lines = java.nio.file.Files.readAllLines(jt.getBasepath().toPath());
    if (lines.isEmpty()) return java.util.Collections.emptyList();
    java.util.List<String> header = new java.util.ArrayList<>(jt.getJqTableLinkedHashMap().keySet());
    java.util.List<java.util.Map<String,String>> rows = new java.util.ArrayList<>();
    for (int i = 1; i < lines.size(); i++) {
      String[] arr = lines.get(i).split(com.dafei1288.jimsql.common.Utils.COLUMN_SPILTOR, -1);
      java.util.LinkedHashMap<String,String> m = new java.util.LinkedHashMap<>();
      for (int c = 0; c < header.size(); c++) m.put(header.get(c), (c < arr.length) ? arr[c] : "");
      rows.add(m);
    }
    if (parts.where != null && !parts.where.trim().isEmpty()) rows = rows.stream().filter(r -> evalWhere(r, jt, parts.where)).collect(java.util.stream.Collectors.toList());
    if (!parts.order.isEmpty()) {
      java.util.Comparator<java.util.Map<String,String>> cmp = (a,b) -> 0;
      for (OrderItem oi : parts.order) {
        String col = normalizeColumn(oi.col);
        int sqlType = columnSqlType(jt, col);
        java.util.Comparator<java.util.Map<String,String>> c = (m1, m2) -> compareValues(getCaseInsensitive(m1,col), getCaseInsensitive(m2,col), sqlType);
        if (!oi.asc) c = c.reversed();
        cmp = cmp.thenComparing(c);
      }
      rows.sort(cmp);
    }
    int off = parts.offset == null ? 0 : Math.max(0, parts.offset);
    Integer lim = parts.limit;
    int from = Math.min(off, rows.size());
    int to = (lim == null) ? rows.size() : Math.min(rows.size(), from + Math.max(0, lim));
    return (from <= to) ? rows.subList(from, to) : java.util.Collections.emptyList();
  }

  private static boolean evalWhere(java.util.Map<String,String> row, com.dafei1288.jimsql.common.meta.JqTable jt, String where) {
    java.util.List<Predicate> preds = parseWhere(where);
    for (Predicate p : preds) { String col = normalizeColumn(p.column); if (!evalOne(getCaseInsensitive(row,col), p, jt)) return false; }
    return true;
  }

  private static java.util.List<Predicate> parseWhere(String where) {
    java.util.List<String> parts = splitByAnd(where);
    java.util.List<Predicate> res = new java.util.ArrayList<>();
    for (String p : parts) { Predicate pr = parsePredicate(p.trim()); if (pr != null) res.add(pr); }
    return res;
  }
  private static java.util.List<String> splitByAnd(String s) {
    java.util.List<String> out = new java.util.ArrayList<>();
    StringBuilder buf = new StringBuilder(); boolean inStr = false; for (int i=0;i<s.length();i++){ char ch=s.charAt(i); if (ch=='\''){ inStr=!inStr; buf.append(ch); continue;} if(!inStr && i+3<=s.length()){ String sub=s.substring(i, Math.min(i+3,s.length())); if (sub.equalsIgnoreCase("AND")) { out.add(buf.toString()); buf.setLength(0); i+=2; continue; } } buf.append(ch);} if(buf.length()>0) out.add(buf.toString()); return out.stream().filter(t -> t!=null && !t.trim().isEmpty()).collect(java.util.stream.Collectors.toList());
  }
  private static Predicate parsePredicate(String s) {
    String[] ops = new String[]{">=","<=","!=","=",">","<"}; String opFound=null; int pos=-1; for (String op:ops){ int idx=indexOfOp(s,op); if(idx>=0){ opFound=op; pos=idx; break; } } if (opFound==null) return null; String lhs=s.substring(0,pos).trim(); String rhs=s.substring(pos+opFound.length()).trim(); Predicate p=new Predicate(); p.column=lhs; p.op=opFound; if (rhs.startsWith("'")) { int end=rhs.lastIndexOf('\''); String content=(end>0)?rhs.substring(1,end):rhs.substring(1); p.literalString=content; p.literalNumeric=null; } else { p.literalString=rhs; try{ p.literalNumeric=new java.math.BigDecimal(rhs);}catch(Exception e){ p.literalNumeric=null; } } return p;
  }
  private static int indexOfOp(String s, String op) { boolean inStr=false; for (int i=0;i<=s.length()-op.length();i++){ char ch=s.charAt(i); if(ch=='\'') inStr=!inStr; if(!inStr && s.regionMatches(true,i,op,0,op.length())) return i; } return -1; }
  private static class Predicate { String column; String op; String literalString; java.math.BigDecimal literalNumeric; }
  private static boolean evalOne(String raw, Predicate p, com.dafei1288.jimsql.common.meta.JqTable jt) {
    if (p == null) return true;
    String col = normalizeColumn(p.column);
    int sqlType = columnSqlType(jt, col);
    if (raw == null) raw = "";
    if (isNumericType(sqlType) && p.literalNumeric != null) {
      try {
        java.math.BigDecimal left = new java.math.BigDecimal(raw.trim());
        java.math.BigDecimal right = p.literalNumeric;
        int c = left.compareTo(right);
        return switchOp(c, p.op);
      } catch (Exception e) { /* fall through */ }
    }
    int c = raw.compareTo(p.literalString);
    return switchOp(c, p.op);
  }
  private static boolean isNumericType(int t) {
    switch (t) {
      case java.sql.Types.INTEGER: case java.sql.Types.BIGINT: case java.sql.Types.SMALLINT: case java.sql.Types.TINYINT:
      case java.sql.Types.DOUBLE: case java.sql.Types.FLOAT: case java.sql.Types.DECIMAL: case java.sql.Types.NUMERIC:
        return true;
      default: return false;
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

  private static String normalizeColumn(String c) { if (c==null) return null; c = stripQuotes(c); int dot=c.lastIndexOf('.'); if (dot>=0) c=c.substring(dot+1); return c; }
  private static String stripQuotes(String s) { if (s==null || s.length()<2) return s; char f=s.charAt(0), l=s.charAt(s.length()-1); if ((f=='`'&&l=='`') || (f=='"'&&l=='"')) return s.substring(1,s.length()-1); return s; }
  private static String getCaseInsensitive(java.util.Map<String,String> row, String col) { if (row==null || col==null) return null; for (String k : row.keySet()) if (k.equalsIgnoreCase(col)) return row.get(k); return null; }
  private static int columnSqlType(com.dafei1288.jimsql.common.meta.JqTable jt, String col) { for (String k : jt.getJqTableLinkedHashMap().keySet()) { if (k.equalsIgnoreCase(col)) { com.dafei1288.jimsql.common.meta.JqColumn jc = jt.getJqTableLinkedHashMap().get(k); if (jc != null) return jc.getColumnType(); } } return java.sql.Types.VARCHAR; }
  private static int compareValues(String v1, String v2, int sqlType) { if (java.util.Objects.equals(v1,v2)) return 0; if (v1==null) return -1; if (v2==null) return 1; switch(sqlType){ case java.sql.Types.INTEGER: case java.sql.Types.BIGINT: case java.sql.Types.SMALLINT: case java.sql.Types.TINYINT: case java.sql.Types.DOUBLE: case java.sql.Types.FLOAT: case java.sql.Types.DECIMAL: case java.sql.Types.NUMERIC: try { java.math.BigDecimal b1=new java.math.BigDecimal(v1.trim()); java.math.BigDecimal b2=new java.math.BigDecimal(v2.trim()); return b1.compareTo(b2);} catch(Exception ignore){} default: return v1.compareTo(v2);} }private void sendTypedBatch(ChannelHandlerContext ctx, long requestId, List<String[]> rows, int[] types) throws java.io.IOException {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    RowBinary.writeTypedBatch(baos, rows, types.length, types);
    byte[] payload = baos.toByteArray();
    short flags = 0;
    Boolean comp = ctx.channel().attr(ATTR_COMPRESS).get();
    if (comp != null && comp) { payload = Compression.lz4(payload); flags |= 0x1; }
    FrameHeader h = new FrameHeader((byte)1, (byte)MessageType.RESULTSET_BATCH.code, flags, requestId, payload.length, 0);
    if ("json".equalsIgnoreCase(WIRELOG)) {
      LOG.info("[wirelog] S->C RESULTSET_BATCH req={} flags={} len={} rows={}", requestId, (int)flags, payload.length, rows.size());
    }
    ctx.writeAndFlush(new ProtocolFrame(h, payload));
  }

  private void sendSimple(ChannelHandlerContext ctx, MessageType type, long requestId, String json) {
    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    FrameHeader h = new FrameHeader((byte)1, (byte)type.code, (short)0, requestId, bytes.length, 0);
    if ("json".equalsIgnoreCase(WIRELOG)) {
      LOG.info("[wirelog] S->C {} req={} flags={} len={} payload={}", type.name(), requestId, 0, bytes.length, json);
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
  private static String sqlTypeToName(int t) {
    switch (t) {
      case java.sql.Types.INTEGER: return "INT";
      case java.sql.Types.BIGINT: return "BIGINT";
      case java.sql.Types.SMALLINT: return "SMALLINT";
      case java.sql.Types.TINYINT: return "TINYINT";
      case java.sql.Types.DOUBLE: return "DOUBLE";
      case java.sql.Types.FLOAT: return "FLOAT";
      case java.sql.Types.DECIMAL:
      case java.sql.Types.NUMERIC: return "DECIMAL";
      default: return "VARCHAR(50)";
    }
  }

    private static String buildCreateTableDDL(String table, java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> cols) {
    String nl = System.lineSeparator();
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE `").append(table).append("` (").append(nl);
    int i = 0;
    for (java.util.Map.Entry<String, com.dafei1288.jimsql.common.meta.JqColumn> e : cols.entrySet()) {
      if (i++ > 0) sb.append(",").append(nl);
      sb.append("  `").append(e.getKey()).append("` ").append(sqlTypeToName(e.getValue().getColumnType()));
    }
    sb.append(nl).append(");");
    return sb.toString();
  }}






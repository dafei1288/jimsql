package com.dafei1288.jimsql.server;

import com.dafei1288.jimsql.server.plan.logical.LlmFunctionSpec;
import com.dafei1288.jimsql.common.JimSQueryStatus;
import com.dafei1288.jimsql.common.JqColumnResultSetMetadata;
import com.dafei1288.jimsql.common.JqQueryReq;
import com.dafei1288.jimsql.common.JqResultSetMetaData;
import com.dafei1288.jimsql.common.RowData;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.SqlParser;
import com.dafei1288.jimsql.server.parser.dql.SelectTableParseTreeProcessor;
import com.dafei1288.jimsql.server.plan.logical.OptimizeQueryLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.LinkedHashMap;
import org.snt.inmemantlr.tree.ParseTreeProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JimServerHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(JimServerHandler.class);
@Override
  public void channelReadComplete(io.netty.channel.ChannelHandlerContext ctx) throws Exception {
    ctx.writeAndFlush(com.dafei1288.jimsql.common.JimSQueryStatus.FINISH);
    ctx.flush();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ctx.writeAndFlush(JimSQueryStatus.BEGIN);
    ctx.flush();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    //super.channelRead(ctx,msg);


    JqQueryReq jqQueryReq = (JqQueryReq)msg;
    JqQueryReq reNew = new JqQueryReq();
    reNew.setDb(jqQueryReq.getDb());
    reNew.setSql(jqQueryReq.getSql());

    String sql = reNew.getSql();
    LOG.info("SQL: {} (ctx={})", sql, ctx.hashCode());

    ScriptParseTreeProcessor scriptParseTreeProcessor = SqlParser.getInstance().parser(reNew);
    ParseTreeProcessor processor = (ParseTreeProcessor) scriptParseTreeProcessor.process();
    //, (SelectTableParseTreeProcessor) processor
    switch(scriptParseTreeProcessor.getSqlStatementEnum()){
      case SELECT_TABLE:this.processQuery(ctx, processor,reNew) ;break;
      case UPDATE_TABLE: this.processUpdate(ctx, processor, reNew); break;
      case DELETE_TABLE: this.processDelete(ctx, processor, reNew); break;
      case INSERT_TABLE: this.processInsert(ctx, processor, reNew); break;
      case SHOW_DATABASES: case SHOW_TABLES: case SHOW_TABLEDESC: case SHOW_CREATE_TABLE: this.processShow(ctx, processor, reNew); break;
      default: ctx.writeAndFlush(JimSQueryStatus.OK); break;
    }
    ctx.flush();
  }


  private void processQuery(ChannelHandlerContext ctx,ParseTreeProcessor processor,
      JqQueryReq jqQueryReq){
    try{
//      System.out.println(processor);
      LOG.debug("processQuery");
            // parse and get query plan (support both DQL wrapper and direct selectTable)
      org.snt.inmemantlr.tree.ParseTreeProcessor cur = ((ScriptParseTreeProcessor)processor).getCurrentParseTreeProcessor();
      QueryLogicalPlan queryLogicalPlan = null;
      if (cur instanceof com.dafei1288.jimsql.server.parser.dql.DqlScriptParseTreeProcessor) {
        Object r = ((com.dafei1288.jimsql.server.parser.dql.DqlScriptParseTreeProcessor) cur).getResult();
        if (r instanceof QueryLogicalPlan) { queryLogicalPlan = (QueryLogicalPlan) r; }
      } else if (cur instanceof SelectTableParseTreeProcessor) {
        queryLogicalPlan = ((SelectTableParseTreeProcessor) cur).getResult();
      }
      if (queryLogicalPlan == null) { throw new IllegalStateException("no plan for SELECT"); }
      // ensure raw SQL present for alias fallback
      try { if (queryLogicalPlan.getRawSql() == null || queryLogicalPlan.getRawSql().isEmpty()) queryLogicalPlan.setRawSql(jqQueryReq.getSql()); } catch (Throwable ignore) {}
      // alias fallback at handler level: if parser did not set selectItems, parse from raw SQL text
      try {
        java.util.List<com.dafei1288.jimsql.server.plan.logical.SelectItem> sis = queryLogicalPlan.getSelectItems();
        if (sis == null || sis.isEmpty()) {
          java.util.List<com.dafei1288.jimsql.server.plan.logical.SelectItem> parsed = parseSelectItemsFromRaw(jqQueryReq.getSql());
          if (parsed != null && !parsed.isEmpty()) {
            queryLogicalPlan.setSelectItems(parsed);
            if (LOG.isDebugEnabled()) {
              LOG.debug("SELECT alias fallback(handler): {}", parsed.stream().map(x -> (x.getAlias()!=null && !x.getAlias().isEmpty())? x.getAlias(): x.getColumnName()).collect(java.util.stream.Collectors.joining(",")));
            }
          }
        }
      } catch (Throwable ignore) {}
      LOG.debug("plan where={}, having={}", queryLogicalPlan.getWhereExpression(), queryLogicalPlan.getHavingExpression());
      if (queryLogicalPlan.getLlmFunctionSpec() != null) {
        LlmFunctionSpec s = queryLogicalPlan.getLlmFunctionSpec();
        LOG.debug("plan llmSpec: label={}, apiType={}, model={}, baseUrl={}", s.getLabel(), s.getApiType(), s.getModel(), s.getBaseUrl());
      }
      if (queryLogicalPlan.getWhereExpression() == null || queryLogicalPlan.getWhereExpression().trim().isEmpty()) {
        String _raw = jqQueryReq.getSql();
        String _wh = extractWhereFromSql(_raw);
        if (_wh != null && !_wh.isEmpty()) {
          queryLogicalPlan.setWhereExpression(_wh);
          LOG.debug("fallback WHERE from raw SQL: {}", _wh);
        }
      }
      // logical plan is ready; prepare to optimize
      
      // optimize logical plan using current database context
      JqDatabase jqDatabase = new JqDatabase();
      jqDatabase.setDatabaseName(jqQueryReq.getDb());
      OptimizeQueryLogicalPlan optimizeQueryLogicalPlan = queryLogicalPlan.optimizeQueryLogicalPlan(jqDatabase);

      // transform to physical plan
      PhysicalPlan queryPysicalPlan = queryLogicalPlan.transform(optimizeQueryLogicalPlan);
      LOG.debug("get jqResultSetMetaData");
      JqResultSetMetaData jqResultSetMetaData = ((OptimizeQueryLogicalPlan)queryPysicalPlan.getLogicalPlan()).getJqResultSetMetaData();

      LOG.debug("write metadata ...");

      // send result-set metadata to client
      ctx.writeAndFlush(jqResultSetMetaData);


      LOG.debug("write data ...");

      queryPysicalPlan.proxyWrite(ctx);
//      List<RowData> datas = new ArrayList<>();
//      for(int i = 0;i<10;i++){
//        RowData rowData = new RowData();
//        rowData.setNext(true);
//        if(i==9){
//          rowData.setNext(false);
//        }
//        LinkedHashMap<String,Object> map = new LinkedHashMap<>();
//        map.put("id","id"+i);
//        map.put("name","name"+i);
//        map.put("age",i);
//        rowData.setDatas(map);
//        ctx.writeAndFlush(rowData);
//      }
    }catch (Exception e){
      LOG.error("processQuery error", e);
    }


  }
  private void processUpdate(io.netty.channel.ChannelHandlerContext ctx, org.snt.inmemantlr.tree.ParseTreeProcessor processor, com.dafei1288.jimsql.common.JqQueryReq jqQueryReq) {
    try {
      org.snt.inmemantlr.tree.ParseTreeProcessor cur = ((com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor)processor).getCurrentParseTreeProcessor();
      com.dafei1288.jimsql.server.plan.logical.UpdateLogicalPlan plan = null;
      if (cur instanceof com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor) {
        Object r = ((com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor) cur).getResult();
        if (r instanceof com.dafei1288.jimsql.server.plan.logical.UpdateLogicalPlan) { plan = (com.dafei1288.jimsql.server.plan.logical.UpdateLogicalPlan) r; }
      }
      if (plan == null) throw new IllegalStateException("no plan for UPDATE");
      String db = jqQueryReq.getDb(); if (db == null || db.isEmpty()) db = "test";
      int cnt = com.dafei1288.jimsql.server.plan.physical.DmlCsvExecutor.executeUpdate(db, plan);
      ctx.writeAndFlush(Integer.valueOf(cnt));
      ctx.writeAndFlush(com.dafei1288.jimsql.common.JimSQueryStatus.OK);
    } catch (Exception e) {
      LOG.error("processQuery error", e);
    }
  }

  private void processInsert(io.netty.channel.ChannelHandlerContext ctx, org.snt.inmemantlr.tree.ParseTreeProcessor processor, com.dafei1288.jimsql.common.JqQueryReq jqQueryReq) {
    try {
      org.snt.inmemantlr.tree.ParseTreeProcessor cur = ((com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor)processor).getCurrentParseTreeProcessor();
      com.dafei1288.jimsql.server.plan.logical.InsertLogicalPlan plan = null;
      if (cur instanceof com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor) {
        Object r = ((com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor) cur).getResult();
        if (r instanceof com.dafei1288.jimsql.server.plan.logical.InsertLogicalPlan) { plan = (com.dafei1288.jimsql.server.plan.logical.InsertLogicalPlan) r; }
      }
      if (plan == null) throw new IllegalStateException("no plan for INSERT");
      String db = jqQueryReq.getDb(); if (db == null || db.isEmpty()) db = "test";
      int cnt = com.dafei1288.jimsql.server.plan.physical.DmlCsvExecutor.executeInsert(db, plan);
      ctx.writeAndFlush(Integer.valueOf(cnt));
      ctx.writeAndFlush(com.dafei1288.jimsql.common.JimSQueryStatus.OK);
    } catch (Exception e) { LOG.error("processQuery error", e); }
  }
  private void processDelete(io.netty.channel.ChannelHandlerContext ctx, org.snt.inmemantlr.tree.ParseTreeProcessor processor, com.dafei1288.jimsql.common.JqQueryReq jqQueryReq) {
    try {
      org.snt.inmemantlr.tree.ParseTreeProcessor cur = ((com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor)processor).getCurrentParseTreeProcessor();
      com.dafei1288.jimsql.server.plan.logical.DeleteLogicalPlan plan = null;
      if (cur instanceof com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor) {
        Object r = ((com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor) cur).getResult();
        if (r instanceof com.dafei1288.jimsql.server.plan.logical.DeleteLogicalPlan) { plan = (com.dafei1288.jimsql.server.plan.logical.DeleteLogicalPlan) r; }
      }
      if (plan == null) throw new IllegalStateException("no plan for DELETE");
      String db = jqQueryReq.getDb(); if (db == null || db.isEmpty()) db = "test";
      int cnt = com.dafei1288.jimsql.server.plan.physical.DmlCsvExecutor.executeDelete(db, plan);
      ctx.writeAndFlush(Integer.valueOf(cnt));
      ctx.writeAndFlush(com.dafei1288.jimsql.common.JimSQueryStatus.OK);
    } catch (Exception e) {
      LOG.error("processQuery error", e);
    }
  }
private void processShow(io.netty.channel.ChannelHandlerContext ctx, org.snt.inmemantlr.tree.ParseTreeProcessor processor, com.dafei1288.jimsql.common.JqQueryReq req) {
    try {
      org.snt.inmemantlr.tree.ParseTreeProcessor cur = ((com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor)processor).getCurrentParseTreeProcessor();
      com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor.DclRequest dr = null;
      if (cur instanceof com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor) {
        Object r = ((com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor) cur).getResult();
        if (r instanceof com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor.DclRequest) dr = (com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor.DclRequest) r;
      }
      if (dr == null) { ctx.writeAndFlush(com.dafei1288.jimsql.common.JimSQueryStatus.OK); return; }
      String db = req.getDb(); if (db == null || db.isEmpty()) db = "test";
      com.dafei1288.jimsql.server.instance.ServerMetadata SM = com.dafei1288.jimsql.server.instance.ServerMetadata.getInstance();
      if (dr.type == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_DATABASES) {
        java.util.List<String> dbs = new java.util.ArrayList<>(SM.getJqDatabaseLinkedHashMap().keySet());
        com.dafei1288.jimsql.common.JqColumnResultSetMetadata c = new com.dafei1288.jimsql.common.JqColumnResultSetMetadata();
        c.setIndex(1); c.setLabelName("database"); c.setTableName(""); c.setColumnType(java.sql.Types.VARCHAR); c.setClazzStr("java.lang.String"); c.setClazz(String.class);
        java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.JqColumnResultSetMetadata> map = new java.util.LinkedHashMap<>(); map.put("database", c);
        com.dafei1288.jimsql.common.JqResultSetMetaData meta = new com.dafei1288.jimsql.common.JqResultSetMetaData(map);
        ctx.writeAndFlush(meta);
        for (String name : dbs) { com.dafei1288.jimsql.common.RowData rd = new com.dafei1288.jimsql.common.RowData(); rd.setNext(true); java.util.LinkedHashMap<String,Object> row = new java.util.LinkedHashMap<>(); row.put("database", name); rd.setDatas(row); ctx.writeAndFlush(rd);}      
        return;
      }
      if (dr.type == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_TABLES) {
        com.dafei1288.jimsql.common.meta.JqDatabase jdb = SM.fetchDatabaseByName(db);
        java.util.List<String> tables = (jdb==null)? java.util.Collections.emptyList() : new java.util.ArrayList<>(jdb.getJqTableListMap().keySet());
        com.dafei1288.jimsql.common.JqColumnResultSetMetadata c = new com.dafei1288.jimsql.common.JqColumnResultSetMetadata();
        c.setIndex(1); c.setLabelName("table"); c.setTableName(""); c.setColumnType(java.sql.Types.VARCHAR); c.setClazzStr("java.lang.String"); c.setClazz(String.class);
        java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.JqColumnResultSetMetadata> map = new java.util.LinkedHashMap<>(); map.put("table", c);
        com.dafei1288.jimsql.common.JqResultSetMetaData meta = new com.dafei1288.jimsql.common.JqResultSetMetaData(map);
        ctx.writeAndFlush(meta);
        for (String t : tables) { com.dafei1288.jimsql.common.RowData rd = new com.dafei1288.jimsql.common.RowData(); rd.setNext(true); java.util.LinkedHashMap<String,Object> row = new java.util.LinkedHashMap<>(); row.put("table", t); rd.setDatas(row); ctx.writeAndFlush(rd);}      
        return;
      }
      if (dr.type == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_TABLEDESC) {
        String t = dr.tableName;
        com.dafei1288.jimsql.common.meta.JqTable jt = SM.fetchTableByName(db, t);
        java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> cols = (jt==null)? new java.util.LinkedHashMap<>() : jt.getJqTableLinkedHashMap();
        com.dafei1288.jimsql.common.JqColumnResultSetMetadata c1 = new com.dafei1288.jimsql.common.JqColumnResultSetMetadata(); c1.setIndex(1); c1.setLabelName("Field"); c1.setColumnType(java.sql.Types.VARCHAR); c1.setClazzStr("java.lang.String"); c1.setClazz(String.class); c1.setTableName(t);
        com.dafei1288.jimsql.common.JqColumnResultSetMetadata c2 = new com.dafei1288.jimsql.common.JqColumnResultSetMetadata(); c2.setIndex(2); c2.setLabelName("Type"); c2.setColumnType(java.sql.Types.VARCHAR); c2.setClazzStr("java.lang.String"); c2.setClazz(String.class); c2.setTableName(t);
        java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.JqColumnResultSetMetadata> map = new java.util.LinkedHashMap<>(); map.put("Field", c1); map.put("Type", c2);
        com.dafei1288.jimsql.common.JqResultSetMetaData meta = new com.dafei1288.jimsql.common.JqResultSetMetaData(map);
        ctx.writeAndFlush(meta);
        for (java.util.Map.Entry<String, com.dafei1288.jimsql.common.meta.JqColumn> e : cols.entrySet()) {
          String name = e.getKey(); int sqlType = e.getValue().getColumnType(); String typeName = sqlTypeToName(sqlType);
          com.dafei1288.jimsql.common.RowData rd = new com.dafei1288.jimsql.common.RowData(); rd.setNext(true); java.util.LinkedHashMap<String,Object> row = new java.util.LinkedHashMap<>(); row.put("Field", name); row.put("Type", typeName); rd.setDatas(row); ctx.writeAndFlush(rd);
        }
        return;
      }
      if (dr.type == com.dafei1288.jimsql.server.parser.SqlStatementEnum.SHOW_CREATE_TABLE) {
        String t = dr.tableName;
        com.dafei1288.jimsql.common.meta.JqTable jt = SM.fetchTableByName(db, t);
        java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> cols = (jt==null)? new java.util.LinkedHashMap<>() : jt.getJqTableLinkedHashMap();
        com.dafei1288.jimsql.common.JqColumnResultSetMetadata c1 = new com.dafei1288.jimsql.common.JqColumnResultSetMetadata(); c1.setIndex(1); c1.setLabelName("Table"); c1.setColumnType(java.sql.Types.VARCHAR); c1.setClazzStr("java.lang.String"); c1.setClazz(String.class);
        com.dafei1288.jimsql.common.JqColumnResultSetMetadata c2 = new com.dafei1288.jimsql.common.JqColumnResultSetMetadata(); c2.setIndex(2); c2.setLabelName("Create Table"); c2.setColumnType(java.sql.Types.VARCHAR); c2.setClazzStr("java.lang.String"); c2.setClazz(String.class);
        java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.JqColumnResultSetMetadata> map = new java.util.LinkedHashMap<>(); map.put("Table", c1); map.put("Create Table", c2);
        com.dafei1288.jimsql.common.JqResultSetMetaData meta = new com.dafei1288.jimsql.common.JqResultSetMetaData(map);
        ctx.writeAndFlush(meta);
        String ddl = buildCreateTableDDL(t, cols);
        com.dafei1288.jimsql.common.RowData rd = new com.dafei1288.jimsql.common.RowData(); rd.setNext(true); java.util.LinkedHashMap<String,Object> row = new java.util.LinkedHashMap<>(); row.put("Table", t); row.put("Create Table", ddl); rd.setDatas(row); ctx.writeAndFlush(rd);
        return;
      }
      ctx.writeAndFlush(com.dafei1288.jimsql.common.JimSQueryStatus.OK);
    } catch (Exception e) { LOG.error("processQuery error", e); }
  }
  private static String extractWhereFromSql(String sql) {
    if (sql == null) return null;
    String s = sql;
    int n = s.length();
    boolean inS = false; char q = 0;
    int wherePos = -1;
    for (int i = 0; i < n; i++) {
      char c = s.charAt(i);
      if (inS) { if (c == q) inS = false; continue; }
      if (c == '\'' || c == '"') { inS = true; q = c; continue; }
      if (i + 5 <= n) {
        String sub = s.substring(i, i + 5);
        if (sub.equalsIgnoreCase("where")) { wherePos = i; break; }
      }
    }
    if (wherePos < 0) return null;
    int start = wherePos + 5;
    int end = n;
    for (int i = start; i < n; i++) {
      char c = s.charAt(i);
      if (inS) { if (c == q) inS = false; continue; }
      if (c == '\'' || c == '"') { inS = true; q = c; continue; }
      if (i + 5 <= n && s.substring(i, Math.min(n, i+5)).equalsIgnoreCase("GROUP")) { end = i; break; }
      if (i + 6 <= n && s.substring(i, Math.min(n, i+6)).equalsIgnoreCase("HAVING")) { end = i; break; }
      if (i + 5 <= n && s.substring(i, Math.min(n, i+5)).equalsIgnoreCase("ORDER")) { end = i; break; }
      if (i + 5 <= n && s.substring(i, Math.min(n, i+5)).equalsIgnoreCase("LIMIT")) { end = i; break; }
    }
    String out = s.substring(start, end).trim();
    if (out.endsWith(";")) out = out.substring(0, out.length()-1).trim();
    if (out.startsWith("=")) out = out.substring(1).trim();
    return out;
  }
private String sqlTypeToName(int t) {
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

    private String buildCreateTableDDL(String table, java.util.LinkedHashMap<String, com.dafei1288.jimsql.common.meta.JqColumn> cols) {
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
  }

  private static java.util.List<com.dafei1288.jimsql.server.plan.logical.SelectItem> parseSelectItemsFromRaw(String whole) {
    if (whole == null || whole.isEmpty()) return new java.util.ArrayList<>();
    java.util.regex.Matcher m = java.util.regex.Pattern.compile("(?is)\\bselect\\s+(.*?)\\s+from\\b").matcher(whole);
    if (!m.find()) return new java.util.ArrayList<>();
    String seg = m.group(1).trim();
    java.util.List<String> parts = new java.util.ArrayList<>();
    int last = 0, depth = 0; boolean inS = false; char q = 0;
    for (int i = 0; i < seg.length(); i++) { char c = seg.charAt(i); if (inS) { if (c == q) inS = false; continue; } if (c == '\'' || c == '"') { inS = true; q = c; continue; } if (c == '(') { depth++; continue; } if (c == ')') { if (depth > 0) depth--; continue; } if (c == ',' && depth == 0) { parts.add(seg.substring(last, i)); last = i + 1; } }
    parts.add(seg.substring(last));
    java.util.List<com.dafei1288.jimsql.server.plan.logical.SelectItem> out = new java.util.ArrayList<>();
    for (String it : parts) {
      String t = it.trim(); if (t.isEmpty()) continue;
      String col = t, alias = null;
      java.util.regex.Matcher mAs = java.util.regex.Pattern.compile("(?is)^(.+?)\\s+as\\s+(.+)$").matcher(t);
      if (mAs.find()) { col = mAs.group(1).trim(); alias = mAs.group(2).trim(); }
      else { int lastWs = -1; boolean inS2 = false; char q2 = 0; int depth2 = 0; for (int i = 0; i < t.length(); i++) { char c = t.charAt(i); if (inS2) { if (c == q2) inS2 = false; continue; } if (c == '\'' || c == '"') { inS2 = true; q2 = c; continue; } if (c == '(') { depth2++; continue; } if (c == ')') { if (depth2 > 0) depth2--; continue; } if (depth2 == 0 && Character.isWhitespace(c)) lastWs = i; } if (lastWs > 0 && lastWs < t.length() - 1) { alias = t.substring(lastWs + 1).trim(); col = t.substring(0, lastWs).trim(); } }
      if (alias != null && !alias.isEmpty()) { if ((alias.startsWith("`") && alias.endsWith("`")) || (alias.startsWith("\"") && alias.endsWith("\""))) { if (alias.length() >= 2) alias = alias.substring(1, alias.length() - 1); } if (alias.isEmpty()) alias = null; }
      com.dafei1288.jimsql.server.plan.logical.SelectItem si = new com.dafei1288.jimsql.server.plan.logical.SelectItem(); si.setColumnName(col); si.setAlias(alias); out.add(si);
    }
    return out;
  }
}

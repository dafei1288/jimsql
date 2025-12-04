package com.dafei1288.jimsql.server;

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


public class JimServerHandler extends ChannelInboundHandlerAdapter {

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
    System.out.println("sql will run : " + sql +" , on "+ctx.hashCode());

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
      System.out.println("processQuery");
      // ????????
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
      //????????????
      
      //???????????????
      JqDatabase jqDatabase = new JqDatabase();
      jqDatabase.setDatabaseName(jqQueryReq.getDb());
      OptimizeQueryLogicalPlan optimizeQueryLogicalPlan = queryLogicalPlan.optimizeQueryLogicalPlan(jqDatabase);

      //????????????
      PhysicalPlan queryPysicalPlan = queryLogicalPlan.transform(optimizeQueryLogicalPlan);
      System.out.println("get jqResultSetMetaData");
      JqResultSetMetaData jqResultSetMetaData = ((OptimizeQueryLogicalPlan)queryPysicalPlan.getLogicalPlan()).getJqResultSetMetaData();

      System.out.println("write metadata .... ");

      //???metadata
      ctx.writeAndFlush(jqResultSetMetaData);


      System.out.println("write adata .... ");

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
      e.printStackTrace();
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
      e.printStackTrace();
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
    } catch (Exception e) { e.printStackTrace(); }
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
      e.printStackTrace();
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
    } catch (Exception e) { e.printStackTrace(); }
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
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE `").append(table).append("` (\n");
    int i = 0;
    for (java.util.Map.Entry<String, com.dafei1288.jimsql.common.meta.JqColumn> e : cols.entrySet()) {
      if (i++ > 0) sb.append(",\n");
      sb.append("  `").append(e.getKey()).append("` ").append(sqlTypeToName(e.getValue().getColumnType()));
    }
    sb.append("\n);");
    return sb.toString();
  }
}

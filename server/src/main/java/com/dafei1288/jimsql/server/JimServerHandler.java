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




  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.writeAndFlush(JimSQueryStatus.FINISH);
    ctx.flush();
  }



  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.getCause();
    ctx.channel().close();
  }

}


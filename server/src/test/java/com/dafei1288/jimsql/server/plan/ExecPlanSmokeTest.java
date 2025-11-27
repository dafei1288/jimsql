package com.dafei1288.jimsql.server.plan;

import com.dafei1288.jimsql.common.JqQueryReq;
import com.dafei1288.jimsql.common.RowData;
import com.dafei1288.jimsql.common.Utils;
import com.dafei1288.jimsql.server.instance.ServerMetadata;
import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.SqlParser;
import com.dafei1288.jimsql.server.parser.dql.SelectTableParseTreeProcessor;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.OptimizeQueryLogicalPlan;
import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;
import com.dafei1288.jimsql.server.plan.physical.QueryPhysicalPlan;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/** Minimal physical-plan smoke: prepares CSV under ./data/test, runs SELECT with WHERE/ORDER/LIMIT. */
public class ExecPlanSmokeTest {
  public static void main(String[] args) throws Exception {
    // Prepare dataset under default data dir (./data)
    File dbDir = new File("data/test");
    dbDir.mkdirs();
    File tbl = new File(dbDir, "user." + Utils.DB_FILENAME_SUFFIX);
    try (PrintWriter pw = new PrintWriter(new FileWriter(tbl))) {
      pw.println("id,name,age,amount");
      pw.println("1,Alice,30,100.5");
      pw.println("2,Bob,19,50");
      pw.println("3,Carol,25,75.25");
      pw.println("4,Dan,40,5");
      pw.println("5,Eve,22,200");
    }

    // Build SELECT
    String sql = "SELECT id, name FROM user WHERE age >= 20 ORDER BY id DESC LIMIT 2 OFFSET 1";
    JqQueryReq req = new JqQueryReq();
    req.setDb("test");
    req.setSql(sql);

    ScriptParseTreeProcessor s = SqlParser.getInstance().parser(req);
    s.process();

      QueryLogicalPlan plan = null;
      org.snt.inmemantlr.tree.ParseTreeProcessor cur = s.getCurrentParseTreeProcessor();
      if (cur instanceof com.dafei1288.jimsql.server.parser.dql.DqlScriptParseTreeProcessor) {
          Object r = ((com.dafei1288.jimsql.server.parser.dql.DqlScriptParseTreeProcessor) cur).getResult();
          if (r instanceof QueryLogicalPlan) plan = (QueryLogicalPlan) r;
      } else if (cur instanceof SelectTableParseTreeProcessor) {
          plan = ((SelectTableParseTreeProcessor) cur).getResult();
      }
      if (plan == null) {
          throw new IllegalStateException(
                  "no plan for SELECT (cur=" +
                          (cur==null ? "null" : cur.getClass().getName()) +
                          ", enum=" + (s.getSqlStatementEnum()==null?"null":s.getSqlStatementEnum().name()) + ")"
          );
      }

      // Optimize with current database
    OptimizeQueryLogicalPlan opt = plan.optimizeQueryLogicalPlan(ServerMetadata.getInstance().fetchDatabaseByName("test"));

    // Physical and fake ctx
    PhysicalPlan pp = plan.transform(opt);

    final List<RowData> out = new ArrayList<>();
    ChannelHandlerContext ctx = (ChannelHandlerContext) Proxy.newProxyInstance(
        ExecPlanSmokeTest.class.getClassLoader(),
        new Class[]{ChannelHandlerContext.class},
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] a) throws Throwable {
            if (method.getName().equals("writeAndFlush") && a != null && a.length >= 1 && a[0] instanceof RowData) {
              out.add((RowData) a[0]);
              return null;
            }
            // return a reasonable default for fluent calls
            Class<?> rt = method.getReturnType();
            if (rt == ChannelHandlerContext.class) return proxy;
            return null;
          }
        }
    );

    ((QueryPhysicalPlan) pp).proxyWrite(ctx);

    System.out.println("Plan: limit="+plan.getLimit()+", offset="+plan.getOffset()+", where="+plan.getWhereExpression());
    System.out.println("Rows: " + out.size());
    for (RowData r : out) {
      System.out.println(r.getDatas());
    }
  }
}
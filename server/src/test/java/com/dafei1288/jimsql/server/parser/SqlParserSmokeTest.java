package com.dafei1288.jimsql.server.parser;

import com.dafei1288.jimsql.common.JqQueryReq;
import com.dafei1288.jimsql.server.parser.dql.SelectTableParseTreeProcessor;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.OrderItem;
import com.dafei1288.jimsql.server.plan.logical.JoinSpec;
import org.snt.inmemantlr.exceptions.*;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

/**
 * Parser smoke test using server SqlParser and processors.
 * Not a JUnit test; run manually if needed.
 */
public class SqlParserSmokeTest {
  public static void main(String[] args) throws Exception {
    String[] sqls = new String[] {
      "SELECT DISTINCT u.id, u.name, SUM(o.amount) AS total FROM `user` u LEFT JOIN `order` o ON o.user_id = u.id WHERE u.status = 'A' AND o.created_at >= '2024-01-01' GROUP BY u.id, u.name HAVING SUM(o.amount) > 100 ORDER BY total DESC, u.id ASC LIMIT 10 OFFSET 20",
      "SELECT id, name FROM a UNION ALL SELECT id, name FROM b",
      "INSERT INTO user(id, name) VALUES (1,'x'),(2,'y')",
      "INSERT INTO user SET id=3, name='z'",
      "SHOW CREATE TABLE `user`",
      "DESCRIBE `user`"
    };

    int ok = 0; int i = 1;
    for (String sql : sqls) {
      try {
        JqQueryReq req = new JqQueryReq();
        req.setDb("test"); req.setSql(sql);
        ScriptParseTreeProcessor s = SqlParser.getInstance().parser(req);
        ParseTreeProcessor proc = (ParseTreeProcessor) s.process();

        System.out.println("[OK] (" + (i++) + ") " + sql);
        ok++;

        if (s.getSqlStatementEnum() != null && s.getSqlStatementEnum().name().equals("SELECT_TABLE")) {
          SelectTableParseTreeProcessor stpp = (SelectTableParseTreeProcessor) ((ScriptParseTreeProcessor)proc).getCurrentParseTreeProcessor();
          QueryLogicalPlan plan = stpp.getResult();
          dumpPlan(plan);
        }
      } catch (Throwable e) {
        System.out.println("[FAIL] (" + (i++) + ") " + sql);
        e.printStackTrace(System.out);
      }
    }
    if (ok != sqls.length) System.exit(2);
  }

  private static void dumpPlan(QueryLogicalPlan p) {
    System.out.println("  fromTable=" + (p.getFromTable()==null?null:p.getFromTable().getTableName()));
    System.out.println("  orderBy=" + p.getOrderBy().stream().map(oi -> oi.getColumn().getColumnName()+" "+(oi.isAsc()?"ASC":"DESC")).toList());
    System.out.println("  limit=" + p.getLimit() + ", offset=" + p.getOffset());
    System.out.println("  groupBy=" + p.getGroupByColumns().stream().map(c->c.getColumnName()).toList());
    System.out.println("  where=" + p.getWhereExpression());
    System.out.println("  having=" + p.getHavingExpression());
    System.out.println("  joins=" + p.getJoins().stream().map(j-> j.getType()+" "+(j.getRightTable()==null?null:j.getRightTable().getTableName())+" ON "+j.getOnExpression()).toList());
  }
}



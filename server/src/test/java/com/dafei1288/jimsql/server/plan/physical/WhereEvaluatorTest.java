package com.dafei1288.jimsql.server.plan.physical;

import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.plan.physical.WhereEvaluator;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class WhereEvaluatorTest {
  private JqTable table() {
    JqTable t = new JqTable();
    java.util.LinkedHashMap<String,JqColumn> cols = new java.util.LinkedHashMap<>();
    cols.put("id", col(java.sql.Types.INTEGER));
    cols.put("name", col(java.sql.Types.VARCHAR));
    cols.put("age", col(java.sql.Types.INTEGER));
    t.setJqTableLinkedHashMap(cols);
    return t;
  }
  private JqColumn col(int type) { JqColumn c = new JqColumn(); c.setColumnType(type); c.setColumnClazzType(Integer.class); return c; }

  private Map<String,String> row(Object... kv) {
    Map<String,String> r = new LinkedHashMap<>();
    for (int i=0; i<kv.length; i+=2) r.put(String.valueOf(kv[i]), String.valueOf(kv[i+1]));
    return r;
  }

  @Test
  public void testOrAndParensLikeInNull() {
    JqTable jt = table();
    Map<String,String> r1 = row("id","1","name","jacky","age","22");
    Map<String,String> r2 = row("id","2","name","doudou","age","3");
    Map<String,String> r3 = row("id","3","name","","age","");

    // OR and AND precedence with parentheses
    WhereEvaluator.Node e1 = WhereEvaluator.parse("age = 3 OR (age = 22 AND name = 'jacky')");
    assertTrue(e1.eval(r1,jt));
    assertTrue(e1.eval(r2,jt));

    // LIKE
    WhereEvaluator.Node e2 = WhereEvaluator.parse("name LIKE 'ja%'");
    assertTrue(e2.eval(r1,jt));
    assertFalse(e2.eval(r2,jt));

    // IN list
    WhereEvaluator.Node e3 = WhereEvaluator.parse("age IN (3,22)");
    assertTrue(e3.eval(r1,jt));
    assertTrue(e3.eval(r2,jt));

    // IS NULL
    WhereEvaluator.Node e4 = WhereEvaluator.parse("age IS NULL");
    assertTrue(e4.eval(r3,jt));

    // NOT LIKE
    WhereEvaluator.Node e5 = WhereEvaluator.parse("name NOT LIKE 'do%'");
    assertTrue(e5.eval(r1,jt));
    assertFalse(e5.eval(r2,jt));
  }
}
package com.dafei1288.jimsql.server.parser;

import com.dafei1288.jimsql.common.JqQueryReq;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SqlParserTest {
  private ScriptParseTreeProcessor parse(String sql) throws Exception {
    JqQueryReq req = new JqQueryReq(); req.setDb("test"); req.setSql(sql);
    ScriptParseTreeProcessor s = SqlParser.getInstance().parser(req);
    s.process();
    return s;
  }

  @Test
  public void testShowParses() throws Exception {
    assertEquals(SqlStatementEnum.SHOW_DATABASES, parse("SHOW DATABASES").getSqlStatementEnum());
    assertEquals(SqlStatementEnum.SHOW_TABLES, parse("SHOW TABLES").getSqlStatementEnum());
    assertNotNull(parse("DESCRIBE `user`").getSqlStatementEnum());
    assertEquals(SqlStatementEnum.SHOW_CREATE_TABLE, parse("SHOW CREATE TABLE `user`").getSqlStatementEnum());
  }

  @Test
  public void testSelectParses() throws Exception {
    ScriptParseTreeProcessor s = parse("SELECT id, name FROM user WHERE age = 3 ORDER BY id DESC LIMIT 2 OFFSET 1");
    assertEquals(SqlStatementEnum.SELECT_TABLE, s.getSqlStatementEnum());
    assertNotNull(s.getCurrentParseTreeProcessor());
  }
}
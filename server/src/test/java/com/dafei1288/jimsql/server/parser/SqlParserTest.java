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
    assertNotNull(parse("SHOW DATABASES").getCurrentParseTreeProcessor());
    assertNotNull(parse("SHOW TABLES").getCurrentParseTreeProcessor());
    assertNotNull(parse("DESCRIBE `user`").getCurrentParseTreeProcessor());
    assertNotNull(parse("SHOW CREATE TABLE `user`").getCurrentParseTreeProcessor());
  }

  @Test
  public void testSelectParses() throws Exception {
    ScriptParseTreeProcessor s = parse("SELECT id, name FROM user WHERE age = 3 ORDER BY id DESC LIMIT 2 OFFSET 1");
    assertEquals(SqlStatementEnum.SELECT_TABLE, s.getSqlStatementEnum());
    assertNotNull(s.getCurrentParseTreeProcessor());
  }
}
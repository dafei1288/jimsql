package com.dafei1288.jimsql.server.parser.dcl;

import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.SqlStatementEnum;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;

public class DclScriptParseTreeProcessor extends ScriptParseTreeProcessor {

  public static class DclRequest {
    public SqlStatementEnum type;
    public String tableName;
  }

  private DclRequest req;

  public DclScriptParseTreeProcessor(ParseTree parseTree) { super(parseTree); }

  @Override
  public Object getResult() { return req; }

  @Override
  protected void process(ParseTreeNode n) {
    if (smap.get(n) == null) {
      ParseTree sub = this.parseTree.getSubtrees(it -> it == n).stream().findFirst().orElse(null);
      if (sub == null) sub = this.parseTree.getSubtrees(it -> it.getRule().equals(n.getRule())).stream().findFirst().orElse(null);
      if (sub != null) smap.put(n, sub);
    }
    String r = n.getRule();
    if ("showDatabases".equals(r)) {
      req = new DclRequest(); req.type = SqlStatementEnum.SHOW_DATABASES; this.setSqlStatementEnum(SqlStatementEnum.SHOW_DATABASES); this.setCurrentParseTreeProcessor(this);
    } else if ("showTables".equals(r)) {
      req = new DclRequest(); req.type = SqlStatementEnum.SHOW_TABLES; this.setSqlStatementEnum(SqlStatementEnum.SHOW_TABLES); this.setCurrentParseTreeProcessor(this);
    } else if ("showCreateTable".equals(r)) {
      req = new DclRequest(); req.type = SqlStatementEnum.SHOW_CREATE_TABLE; this.setSqlStatementEnum(SqlStatementEnum.SHOW_CREATE_TABLE); this.setCurrentParseTreeProcessor(this);
      String t = findTableName(n); if (t != null) req.tableName = stripQuotes(t);
    } else if ("describeTable".equals(r) || "showTableDesc".equals(r)) {
      req = new DclRequest(); req.type = SqlStatementEnum.SHOW_TABLEDESC; this.setSqlStatementEnum(SqlStatementEnum.SHOW_TABLEDESC); this.setCurrentParseTreeProcessor(this);
      String t = findTableName(n); if (t != null) req.tableName = stripQuotes(t);
    }
  }

  private String findTableName(ParseTreeNode n) {
    if ("tableName".equals(n.getRule())) return n.getLabel();
    for (ParseTreeNode c : n.getChildren()) {
      String v = findTableName(c); if (v != null) return v;
    }
    return null;
  }

  private String stripQuotes(String s) {
    if (s == null || s.length() < 2) return s;
    char f = s.charAt(0), l = s.charAt(s.length()-1);
    if ((f == '`' && l == '`') || (f == '"' && l == '"')) return s.substring(1, s.length()-1);
    return s;
  }
}

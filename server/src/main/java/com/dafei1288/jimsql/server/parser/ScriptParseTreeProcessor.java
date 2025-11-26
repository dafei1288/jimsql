package com.dafei1288.jimsql.server.parser;

import com.dafei1288.jimsql.server.parser.dcl.DclScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.ddl.DdlScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.dml.DmlScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.dql.DqlScriptParseTreeProcessor;
import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

public class ScriptParseTreeProcessor extends ParseTreeProcessor {
  private SqlStatementEnum sqlStatementEnum = null;

  public SqlStatementEnum getSqlStatementEnum() {
    if (sqlStatementEnum != null) {
      return sqlStatementEnum;
    }
    ParseTreeProcessor child = this.getCurrentParseTreeProcessor();
    if (child instanceof ScriptParseTreeProcessor) {
      return ((ScriptParseTreeProcessor) child).getSqlStatementEnum();
    }
    return null;
  }

  public void setSqlStatementEnum(SqlStatementEnum sqlStatementEnum) {
    this.sqlStatementEnum = sqlStatementEnum;
  }

  public ParseTreeProcessor getCurrentParseTreeProcessor() {
    return currentParseTreeProcessor;
  }

  public void setCurrentParseTreeProcessor(
      ParseTreeProcessor currentParseTreeProcessor) {
    this.currentParseTreeProcessor = currentParseTreeProcessor;
  }

  int cnt = 0;
  private ParseTreeProcessor currentParseTreeProcessor;

  public ScriptParseTreeProcessor(ParseTree parseTree) {
    super(parseTree);
  }

  @Override
  public Object getResult() {
    return currentParseTreeProcessor;
  }

  @Override
  protected void initialize() {
//    System.out.println(this+" => initialize()");
    for (ParseTreeNode n : this.parseTree.getNodes()) {
//      System.out.println("initialize : "+n.getRule());
      ParseTree parseTree1 = this.parseTree.getSubtrees(it->{return  it.getRule().equals(n.getRule());}).stream().findFirst().get();
      smap.put(n, parseTree1);
    }
  }

  @Override
  protected void process(ParseTreeNode parseTreeNode) throws ParseTreeProcessorException {
    cnt++;
    simpleProp(parseTreeNode);

    if("dcl".equals(parseTreeNode.getRule())){
      currentParseTreeProcessor = new DclScriptParseTreeProcessor((ParseTree) smap.get(parseTreeNode));
//      System.out.println("parseTreeNode.getChild(0) -->"+parseTreeNode.getChild(0).getRule());
      currentParseTreeProcessor.process();
    }else if("dql".equals(parseTreeNode.getRule())){
      currentParseTreeProcessor = new DqlScriptParseTreeProcessor((ParseTree) smap.get(parseTreeNode));
      currentParseTreeProcessor.process();
//      System.out.println("parseTreeNode.getChild(0) -->"+parseTreeNode.getChild(0).getRule());
    }else if("dml".equals(parseTreeNode.getRule())){
      currentParseTreeProcessor = new DmlScriptParseTreeProcessor((ParseTree) smap.get(parseTreeNode));
      currentParseTreeProcessor.process();
    }else if("ddl".equals(parseTreeNode.getRule())){
      currentParseTreeProcessor = new DdlScriptParseTreeProcessor((ParseTree) smap.get(parseTreeNode));
      currentParseTreeProcessor.process();
    }



  }
}



package com.dafei1288.jimsql.server.parser.dql;

import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.SqlStatementEnum;
import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

public class DqlScriptParseTreeProcessor extends ScriptParseTreeProcessor {

//  private ParseTreeProcessor currentParseTreeProcessor;

  public DqlScriptParseTreeProcessor(ParseTree parseTree) {
    super(parseTree);
  }

  @Override
  public Object getResult() {
    if (this.getCurrentParseTreeProcessor() != null) {
      return this.getCurrentParseTreeProcessor().getResult();
    }
    // Fallback: try to locate selectTable subtree
    for (ParseTreeNode n : this.parseTree.getNodes()) {
      if ("selectTable".equals(n.getRule())) {
        ParseTree sub = (ParseTree) smap.get(n);
        if (sub != null) {
          SelectTableParseTreeProcessor p = new SelectTableParseTreeProcessor(sub);
          this.setCurrentParseTreeProcessor(p);
          this.setSqlStatementEnum(SqlStatementEnum.SELECT_TABLE);
          try { p.process(); } catch (Exception ignore) {}
          return p.getResult();
        }
      }
    }
    return null;
  }


  @Override
  protected void process(ParseTreeNode parseTreeNode) throws ParseTreeProcessorException {
    if("selectTable".equals(parseTreeNode.getRule())){
//      System.out.println("this is DqlScriptParseTreeProcessor");
      SelectTableParseTreeProcessor processor = new SelectTableParseTreeProcessor((ParseTree) smap.get(parseTreeNode));
      this.setCurrentParseTreeProcessor(processor);
      this.setSqlStatementEnum(SqlStatementEnum.SELECT_TABLE);
      this.getCurrentParseTreeProcessor().process();
//      System.out.println();
    }
  }
}



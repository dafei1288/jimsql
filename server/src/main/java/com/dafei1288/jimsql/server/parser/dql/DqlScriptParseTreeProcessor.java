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

    return this.getCurrentParseTreeProcessor().getResult();
  }


  @Override
  protected void process(ParseTreeNode parseTreeNode) throws ParseTreeProcessorException {
    if("selectTable".equals(parseTreeNode.getRule())){
//      System.out.println("this is DqlScriptParseTreeProcessor");
      SelectTableParseTreeProcessor processor = new SelectTableParseTreeProcessor(parseTree);
      this.setCurrentParseTreeProcessor(processor);
      this.setSqlStatementEnum(SqlStatementEnum.SELECT_TABLE);
      this.getCurrentParseTreeProcessor().process();
//      System.out.println();
    }
  }
}

package com.dafei1288.jimsql.server.parser.ddl;

import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

public class DdlScriptParseTreeProcessor extends ParseTreeProcessor {

  public DdlScriptParseTreeProcessor(ParseTree parseTree) {
    super(parseTree);
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  protected void initialize() {

  }

  @Override
  protected void process(ParseTreeNode parseTreeNode) throws ParseTreeProcessorException {
    System.out.println(parseTreeNode.getRule());
  }
}

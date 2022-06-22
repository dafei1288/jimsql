package com.dafei1288.jimsql.server.parser.dcl;

import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

public class DclScriptParseTreeProcessor extends ParseTreeProcessor {

  public DclScriptParseTreeProcessor(ParseTree parseTree) {
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
    System.out.println("--> "+parseTreeNode.getRule());
  }
}

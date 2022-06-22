package com.dafei1288.jimsql.server.parser.dml;

import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

public class DmlScriptParseTreeProcessor extends ParseTreeProcessor {

  public DmlScriptParseTreeProcessor(ParseTree parseTree) {
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

  }
}

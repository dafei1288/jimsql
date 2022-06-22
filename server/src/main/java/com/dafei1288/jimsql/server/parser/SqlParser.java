package com.dafei1288.jimsql.server.parser;

import com.dafei1288.jimsql.common.JqQueryReq;
import java.io.IOException;
import java.io.InputStream;
import org.snt.inmemantlr.GenericParser;
import org.snt.inmemantlr.exceptions.CompilationException;
import org.snt.inmemantlr.exceptions.IllegalWorkflowException;
import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.exceptions.ParsingException;
import org.snt.inmemantlr.listener.DefaultTreeListener;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeProcessor;
import org.snt.inmemantlr.utils.FileUtils;

public class SqlParser {
  private static SqlParser instance;
  private ParseTree pt;
  private GenericParser gp;

  private JqQueryReq jqQueryReq;

  public static SqlParser getInstance() throws IOException, CompilationException{
    if(instance == null){
      synchronized (SqlParser.class){
        instance = new SqlParser();
      }
    }
    return instance;
  }

  private DefaultTreeListener defaultTreeListener;
  private SqlParser() throws IOException, CompilationException {
    String sgrammarcontent = "";
    try (InputStream sgrammar = SqlParser.class.getResourceAsStream("/jimsql.g4")) {
      sgrammarcontent = FileUtils.getStringFromStream(sgrammar);
      gp = new GenericParser(sgrammarcontent);
      defaultTreeListener = new DefaultTreeListener();

      gp.setListener(defaultTreeListener);
      gp.compile();

    }

  }


  public ScriptParseTreeProcessor parser(JqQueryReq jqQueryReq)
      throws ParsingException, IllegalWorkflowException, ParseTreeProcessorException {
    this.jqQueryReq = jqQueryReq;
    gp.parse(jqQueryReq.getSql());
    pt = defaultTreeListener.getParseTree();
    ScriptParseTreeProcessor scriptParseTreeProcessor = new ScriptParseTreeProcessor(pt);
//    scriptParseTreeProcessor.process();
    return scriptParseTreeProcessor;
  }
}

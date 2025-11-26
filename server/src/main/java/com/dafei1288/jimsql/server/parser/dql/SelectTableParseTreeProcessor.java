package com.dafei1288.jimsql.server.parser.dql;

import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

public class SelectTableParseTreeProcessor extends ScriptParseTreeProcessor {

  private QueryLogicalPlan queryLogicalPlan = new QueryLogicalPlan();

  public SelectTableParseTreeProcessor(ParseTree parseTree) {
    super(parseTree);
  }

  @Override
  public QueryLogicalPlan getResult() {
    return queryLogicalPlan;
  }



  @Override
  protected void process(ParseTreeNode parseTreeNode) throws ParseTreeProcessorException {
//    System.out.println(parseTreeNode.getRule());
    if(parseTreeNode.getRule().equals("columnList")){
      if(parseTreeNode.getLabel().equals("*")){
        queryLogicalPlan.setStar(true);
      }else {
        queryLogicalPlan.setStar(false);
        List<JqColumn> jqColumnList = parseTreeNode.getChildren().stream().filter(it -> "columnName".equals(it.getRule())).map(it -> {
                                                    JqColumn jqColumn = new JqColumn();
                                                    jqColumn.setColumnName(stripQuotes(it.getLabel()));
                                                    return jqColumn;
        }).collect(Collectors.toList());
        queryLogicalPlan.setJqColumnList(jqColumnList);

      }
    }
    if(parseTreeNode.getRule().equals("tableName")){
      JqTable jqTable = new JqTable();
      jqTable.setTableName(stripQuotes(parseTreeNode.getLabel()));
      queryLogicalPlan.setFromTable(jqTable);
    }

  }
}



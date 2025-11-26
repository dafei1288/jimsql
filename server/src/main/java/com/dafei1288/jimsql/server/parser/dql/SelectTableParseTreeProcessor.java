package com.dafei1288.jimsql.server.parser.dql;

import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.OrderItem;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

public class SelectTableParseTreeProcessor extends ScriptParseTreeProcessor {

  
  private boolean limitNext = false;
  private boolean offsetNext = false;public SelectTableParseTreeProcessor(ParseTree parseTree) {
    super(parseTree);
  }

  @Override
  public QueryLogicalPlan getResult() {
    return queryLogicalPlan;
  }



  @Override
    protected void process(ParseTreeNode parseTreeNode) throws ParseTreeProcessorException {
    // existing columns and tableName handling
    if(parseTreeNode.getRule().equals("columnList")){
      if(parseTreeNode.getLabel().equals("*")){
        queryLogicalPlan.setStar(true);
      }else {
        queryLogicalPlan.setStar(false);
        java.util.List<JqColumn> jqColumnList = parseTreeNode.getChildren().stream().filter(it -> "columnName".equals(it.getRule())).map(it -> {
                                                    JqColumn jqColumn = new JqColumn();
                                                    jqColumn.setColumnName(stripQuotes(it.getLabel()));
                                                    return jqColumn;
        }).collect(java.util.stream.Collectors.toList());
        queryLogicalPlan.setJqColumnList(jqColumnList);

      }
    }
    if(parseTreeNode.getRule().equals("tableName")){
      if (queryLogicalPlan.getFromTable() == null) {
        JqTable jqTable = new JqTable();
        jqTable.setTableName(stripQuotes(parseTreeNode.getLabel()));
        queryLogicalPlan.setFromTable(jqTable);
      }
    }

    // ORDER BY items
    if (parseTreeNode.getRule().equals("orderItem")) {
      JqColumn c = new JqColumn();
      boolean asc = true; // default
      for (ParseTreeNode ch : parseTreeNode.getChildren()) {
        if ("columnName".equals(ch.getRule())) {
          c.setColumnName(stripQuotes(ch.getLabel()));
        } else if ("ASC_SYMBOL".equals(ch.getRule()) || "ASC".equalsIgnoreCase(ch.getLabel())) {
          asc = true;
        } else if ("DESC_SYMBOL".equals(ch.getRule()) || "DESC".equalsIgnoreCase(ch.getLabel())) {
          asc = false;
        }
      }
      OrderItem oi = new OrderItem(c, asc);
      queryLogicalPlan.getOrderBy().add(oi);
    }

    // LIMIT/OFFSET
    if ("LIMIT_SYMBOL".equals(parseTreeNode.getRule())) {
      limitNext = true; offsetNext = false; return; // wait for next INT_LITERAL
    }
    if ("OFFSET_SYMBOL".equals(parseTreeNode.getRule())) {
      offsetNext = true; limitNext = false; return;
    }
    if ("INT_LITERAL".equals(parseTreeNode.getRule())) {
      String t = parseTreeNode.getLabel();
      try {
        int v = Integer.parseInt(t);
        if (limitNext && queryLogicalPlan.getLimit() == null) {
          queryLogicalPlan.setLimit(v);
          limitNext = false;
        } else if (offsetNext && queryLogicalPlan.getOffset() == null) {
          queryLogicalPlan.setOffset(v);
          offsetNext = false;
        }
      } catch (Exception ignore) {}
    }
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
      if (queryLogicalPlan.getFromTable() == null) {
        JqTable jqTable = new JqTable();
        jqTable.setTableName(stripQuotes(parseTreeNode.getLabel()));
        queryLogicalPlan.setFromTable(jqTable);
      }
    }

  }

  // Unquote identifiers wrapped in backticks or double quotes
  private String stripQuotes(String s) {
    if (s == null || s.length() < 2) return s;
    char f = s.charAt(0), l = s.charAt(s.length()-1);
    if ((f == '`' && l == '`') || (f == '"' && l == '"')) {
      return s.substring(1, s.length()-1);
    }
    return s;
  }
}


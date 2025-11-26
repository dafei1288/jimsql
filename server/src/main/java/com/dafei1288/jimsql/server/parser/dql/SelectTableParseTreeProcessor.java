package com.dafei1288.jimsql.server.parser.dql;

import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.OrderItem;
import com.dafei1288.jimsql.server.plan.logical.JoinSpec;
import com.dafei1288.jimsql.server.plan.logical.JoinType;
import java.util.List;
import java.util.stream.Collectors;
import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;

public class SelectTableParseTreeProcessor extends ScriptParseTreeProcessor {

  private final QueryLogicalPlan queryLogicalPlan = new QueryLogicalPlan();
  private boolean limitNext = false;
  private boolean offsetNext = false;

  public SelectTableParseTreeProcessor(ParseTree parseTree) {
    super(parseTree);
  }

  @Override
  public QueryLogicalPlan getResult() {
    return queryLogicalPlan;
  }

  @Override
  protected void process(ParseTreeNode parseTreeNode) throws ParseTreeProcessorException {
    // Columns
    if ("columnList".equals(parseTreeNode.getRule())) {
      if ("*".equals(parseTreeNode.getLabel())) {
        queryLogicalPlan.setStar(true);
      } else {
        queryLogicalPlan.setStar(false);
        List<JqColumn> jqColumnList = parseTreeNode.getChildren().stream()
            .filter(it -> "columnName".equals(it.getRule()))
            .map(it -> {
              JqColumn jqColumn = new JqColumn();
              jqColumn.setColumnName(stripQuotes(it.getLabel()));
              return jqColumn;
            }).collect(Collectors.toList());
        queryLogicalPlan.setJqColumnList(jqColumnList);
      }
    }

    // FROM first table
    if ("tableName".equals(parseTreeNode.getRule())) {
      if (queryLogicalPlan.getFromTable() == null) {
        JqTable jqTable = new JqTable();
        jqTable.setTableName(stripQuotes(parseTreeNode.getLabel()));
        queryLogicalPlan.setFromTable(jqTable);
      }
    }

    // ORDER BY
    if ("orderItem".equals(parseTreeNode.getRule())) {
      JqColumn col = new JqColumn();
      boolean asc = true;
      for (ParseTreeNode ch : parseTreeNode.getChildren()) {
        if ("columnName".equals(ch.getRule())) {
          col.setColumnName(stripQuotes(ch.getLabel()));
        } else if ("ASC_SYMBOL".equals(ch.getRule()) || "ASC".equalsIgnoreCase(ch.getLabel())) {
          asc = true;
        } else if ("DESC_SYMBOL".equals(ch.getRule()) || "DESC".equalsIgnoreCase(ch.getLabel())) {
          asc = false;
        }
      }
      queryLogicalPlan.getOrderBy().add(new OrderItem(col, asc));
    }

    // LIMIT / OFFSET
    if ("LIMIT_SYMBOL".equals(parseTreeNode.getRule())) {
      limitNext = true; offsetNext = false; return;
    }
    if ("OFFSET_SYMBOL".equals(parseTreeNode.getRule())) {
      offsetNext = true; limitNext = false; return;
    }
    if ("INT_LITERAL".equals(parseTreeNode.getRule())) {
      try {
        int v = Integer.parseInt(parseTreeNode.getLabel());
        if (limitNext && queryLogicalPlan.getLimit() == null) { queryLogicalPlan.setLimit(v); limitNext = false; }
        else if (offsetNext && queryLogicalPlan.getOffset() == null) { queryLogicalPlan.setOffset(v); offsetNext = false; }
      } catch (Exception ignore) {}
    }

    // JOINs
    if ("tableJoin".equals(parseTreeNode.getRule())) {
      JoinSpec js = new JoinSpec();
      js.setType(JoinType.INNER);
      for (ParseTreeNode ch : parseTreeNode.getChildren()) {
        String r = ch.getRule();
        String lbl = ch.getLabel() == null ? "" : ch.getLabel();
        if ("CROSS_SYMBOL".equals(r) || "CROSS".equalsIgnoreCase(lbl)) {
          js.setType(JoinType.CROSS);
        } else if ("LEFT_SYMBOL".equals(r) || "LEFT".equalsIgnoreCase(lbl)) {
          js.setType(JoinType.LEFT);
        } else if ("RIGHT_SYMBOL".equals(r) || "RIGHT".equalsIgnoreCase(lbl)) {
          js.setType(JoinType.RIGHT);
        } else if ("FULL_SYMBOL".equals(r) || "FULL".equalsIgnoreCase(lbl)) {
          js.setType(JoinType.FULL);
        } else if ("tablePrimary".equals(r)) {
          for (ParseTreeNode pch : ch.getChildren()) {
            if ("tableName".equals(pch.getRule())) {
              JqTable t = new JqTable();
              t.setTableName(stripQuotes(pch.getLabel()));
              js.setRightTable(t);
            } else if ("alias".equals(pch.getRule()) || "identifier".equals(pch.getRule())) {
              js.setAlias(stripQuotes(pch.getLabel()));
            }
          }
        } else if ("expression".equals(r)) {
          js.setOnExpression(extractText(ch));
        }
      }
      queryLogicalPlan.getJoins().add(js);
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

  // Collect textual content from a subtree (best-effort)
  private String extractText(ParseTreeNode node) {
    StringBuilder sb = new StringBuilder();
    dfs(node, sb);
    return sb.toString().trim().replaceAll("\\s+", " ");
  }

  private void dfs(ParseTreeNode n, StringBuilder sb) {
    String lbl = n.getLabel();
    if (lbl != null && !lbl.isEmpty()) {
      if (sb.length() > 0) sb.append(' ');
      sb.append(lbl);
    }
    for (ParseTreeNode c : n.getChildren()) dfs(c, sb);
  }
}

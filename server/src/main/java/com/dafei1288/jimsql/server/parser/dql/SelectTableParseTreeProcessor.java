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
  private boolean whereNext = false;
  private boolean havingNext = false;


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
              // Collect columns from valueExpr subtree
              java.util.List<JqColumn> cols = new java.util.ArrayList<>();
              collectSelectColumns(parseTreeNode, cols);
              if (!cols.isEmpty()) {
                  queryLogicalPlan.setStar(false);
                  queryLogicalPlan.setJqColumnList(cols);
              }
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

    // Fallback: derive WHERE/LIMIT/OFFSET from selectBody text if not captured
    if ("selectBody".equals(parseTreeNode.getRule())) {
        java.util.List<ParseTreeNode> cs = parseTreeNode.getChildren();
        for (int i = 0; i < cs.size(); i++) {
            String r = cs.get(i).getRule();
            if ("WHERE_SYMBOL".equals(r) && queryLogicalPlan.getWhereExpression() == null) {
                for (int j = i + 1; j < cs.size(); j++) {
                    if ("expression".equals(cs.get(j).getRule())) {
                        queryLogicalPlan.setWhereExpression(extractText(cs.get(j)));
                        break;
                    }
                }
            }
            if ("LIMIT_SYMBOL".equals(r) && queryLogicalPlan.getLimit() == null) {
                for (int j = i + 1; j < cs.size(); j++) {
                    if ("INT_LITERAL".equals(cs.get(j).getRule())) {
                        try { queryLogicalPlan.setLimit(Integer.parseInt(cs.get(j).getLabel())); } catch (Exception ignore) {}
                        break;
                    }
                }
            }
            if ("OFFSET_SYMBOL".equals(r) && queryLogicalPlan.getOffset() == null) {
                for (int j = i + 1; j < cs.size(); j++) {
                    if ("INT_LITERAL".equals(cs.get(j).getRule())) {
                        try { queryLogicalPlan.setOffset(Integer.parseInt(cs.get(j).getLabel())); } catch (Exception ignore) {}
                        break;
                    }
                }
            }
        }
        String txt = extractText(parseTreeNode);
      if (txt != null) {
        String upper = txt.toUpperCase(java.util.Locale.ROOT);
        // WHERE ... (GROUP BY|HAVING|ORDER BY|LIMIT|$)
        if (queryLogicalPlan.getWhereExpression() == null) {
          int w = upper.indexOf(" WHERE ");
          if (w >= 0) {
            int end = upper.length();
            for (String kw : new String[]{" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT "}) {
              int k = upper.indexOf(kw, w+1);
              if (k >= 0 && k < end) end = k;
            }
            if (end > w+7) {
              String we = txt.substring(w+7, end).trim();
              if (!we.isEmpty()) queryLogicalPlan.setWhereExpression(we);
            }
          }
        }
        // LIMIT
        if (queryLogicalPlan.getLimit() == null) {
          int l = upper.indexOf(" LIMIT ");
          if (l >= 0) {
            String tail = upper.substring(l+7).trim();
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("^([0-9]+)").matcher(tail);
            if (m.find()) {
              try { queryLogicalPlan.setLimit(Integer.parseInt(m.group(1))); } catch (Exception ignore) {}
            }
          }
        }
        // OFFSET
        if (queryLogicalPlan.getOffset() == null) {
          int o = upper.indexOf(" OFFSET ");
          if (o >= 0) {
            String tail = upper.substring(o+8).trim();
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("^([0-9]+)").matcher(tail);
            if (m.find()) {
              try { queryLogicalPlan.setOffset(Integer.parseInt(m.group(1))); } catch (Exception ignore) {}
            }
          }
        }
      }
    }    // ORDER BY
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
    // GROUP BY
      if ("groupByList".equals(parseTreeNode.getRule())) {
          java.util.List<JqColumn> gcols = new java.util.ArrayList<>();
          for (org.snt.inmemantlr.tree.ParseTreeNode ch : parseTreeNode.getChildren()) {
              if ("columnName".equals(ch.getRule())) {
                  JqColumn c = new JqColumn();
                  c.setColumnName(stripQuotes(ch.getLabel()));
                  gcols.add(c);
              }
          }
          if (!gcols.isEmpty()) {
              java.util.List<JqColumn> all = queryLogicalPlan.getGroupByColumns();
              all.addAll(gcols);
              queryLogicalPlan.setGroupByColumns(all);
          }
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
    // WHERE / HAVING markers
    if ("WHERE_SYMBOL".equals(parseTreeNode.getRule())) {
            whereNext = true;
            havingNext = false; return;
    }
    if ("HAVING_SYMBOL".equals(parseTreeNode.getRule())) {
        havingNext = true;
        whereNext = false; return;
    }
    // Capture expression for WHERE/HAVING (raw text)\n
    if ("expression".equals(parseTreeNode.getRule())) {
        if (whereNext && queryLogicalPlan.getWhereExpression() == null) {
            queryLogicalPlan.setWhereExpression(extractText(parseTreeNode));
            whereNext = false;
        } else if (
                havingNext && queryLogicalPlan.getHavingExpression() == null) {
                    queryLogicalPlan.setHavingExpression(extractText(parseTreeNode));
                    havingNext = false;
        }
    }    // JOINs
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
  if (lbl == null || lbl.isEmpty()) {
    String r = n.getRule();
    if (r != null && r.endsWith("_SYMBOL")) {
      lbl = r.substring(0, r.length() - "_SYMBOL".length());
    }
  }
  if (lbl != null && !lbl.isEmpty()) {
    if (sb.length() > 0) sb.append(' ');
    sb.append(lbl);
  }
  for (ParseTreeNode c : n.getChildren()) dfs(c, sb);
}
    for (ParseTreeNode c : n.getChildren()) dfs(c, sb);
  }
  private void collectSelectColumns(org.snt.inmemantlr.tree.ParseTreeNode node, java.util.List<com.dafei1288.jimsql.common.meta.JqColumn> out) {
  if ("columnName".equals(node.getRule())) {
    com.dafei1288.jimsql.common.meta.JqColumn c = new com.dafei1288.jimsql.common.meta.JqColumn();
    c.setColumnName(stripQuotes(node.getLabel()));
    out.add(c);
    return;
  }
  if ("qualifiedName".equals(node.getRule())) {
    String last = null;
    for (org.snt.inmemantlr.tree.ParseTreeNode ch : node.getChildren()) {
      if ("identifier".equals(ch.getRule())) {
        last = stripQuotes(ch.getLabel());
      }
    }
    if (last != null) {
      com.dafei1288.jimsql.common.meta.JqColumn c = new com.dafei1288.jimsql.common.meta.JqColumn();
      c.setColumnName(last);
      out.add(c);
      return;
    }
  }
  for (org.snt.inmemantlr.tree.ParseTreeNode ch : node.getChildren()) {
    collectSelectColumns(ch, out);
  }
}
    for (org.snt.inmemantlr.tree.ParseTreeNode ch : node.getChildren()) {
      collectSelectColumns(ch, out);
    }
  }}



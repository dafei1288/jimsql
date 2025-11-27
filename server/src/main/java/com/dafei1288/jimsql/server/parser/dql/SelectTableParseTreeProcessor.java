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
    // Ensure we have WHERE/LIMIT/OFFSET even if specific node handlers missed them
    try {
      if (queryLogicalPlan.getWhereExpression() == null || queryLogicalPlan.getLimit() == null || queryLogicalPlan.getOffset() == null) {
        String whole = extractText(this.parseTree.getRoot());
        if (whole != null) {
          String U = whole.toUpperCase(java.util.Locale.ROOT);
          if (queryLogicalPlan.getWhereExpression() == null) {
            int w = U.indexOf(" WHERE ");
            if (w >= 0) {
              int end = U.length();
              for (String kw : new String[]{" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT "}) {
                int k = U.indexOf(kw, w+1);
                if (k >= 0 && k < end) end = k;
              }
              if (end > w+7) {
                String we = whole.substring(w+7, end).trim();
                if (!we.isEmpty()) queryLogicalPlan.setWhereExpression(we);
              }
            }
          }
          if (queryLogicalPlan.getLimit() == null) {
            int l = U.indexOf(" LIMIT ");
            if (l >= 0) {
              String tail = U.substring(l+7).trim();
              java.util.regex.Matcher m2 = java.util.regex.Pattern.compile("^([0-9]+)").matcher(tail);
              if (m2.find()) {
                try { queryLogicalPlan.setLimit(Integer.parseInt(m2.group(1))); } catch (Exception ignore) {}
              }
            }
          }
          if (queryLogicalPlan.getOffset() == null) {
            int o = U.indexOf(" OFFSET ");
            if (o >= 0) {
              String tailo = U.substring(o+8).trim();
              java.util.regex.Matcher mo = java.util.regex.Pattern.compile("^([0-9]+)").matcher(tailo);
              if (mo.find()) {
                try { queryLogicalPlan.setOffset(Integer.parseInt(mo.group(1))); } catch (Exception ignore) {}
              }
            }
          }
        }
      }
    } catch (Exception ignore) {}
    finalizeClausesFromTokens(this.parseTree.getRoot()); finalizeClausesFromText(this.parseTree.getRoot());
    if (queryLogicalPlan.getWhereExpression() == null || queryLogicalPlan.getLimit() == null || queryLogicalPlan.getOffset() == null) {
      java.util.List<String> _toks = new java.util.ArrayList<>();
      flattenTokens(this.parseTree.getRoot(), _toks);
      System.out.println("DBG TOKENS=" + String.join(" ", _toks));
      System.out.println("DBG TEXT=" + extractText(this.parseTree.getRoot()));
    }
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
    }    if ("selectTable".equals(parseTreeNode.getRule())) {
      String txt2 = extractText(parseTreeNode);
      if (txt2 != null) {
        String upper2 = txt2.toUpperCase(java.util.Locale.ROOT);
        if (queryLogicalPlan.getWhereExpression() == null) {
          int w2 = upper2.indexOf(" WHERE ");
          if (w2 >= 0) {
            int end2 = upper2.length();
            for (String kw : new String[]{" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT "}) {
              int k2 = upper2.indexOf(kw, w2+1);
              if (k2 >= 0 && k2 < end2) end2 = k2;
            }
            if (end2 > w2+7) {
              String we2 = txt2.substring(w2+7, end2).trim();
              if (!we2.isEmpty()) queryLogicalPlan.setWhereExpression(we2);
            }
          }
        }
        if (queryLogicalPlan.getLimit() == null) {
          int l2 = upper2.indexOf(" LIMIT ");
          if (l2 >= 0) {
            String tail2 = upper2.substring(l2+7).trim();
            java.util.regex.Matcher m2 = java.util.regex.Pattern.compile("^([0-9]+)").matcher(tail2);
            if (m2.find()) {
              try { queryLogicalPlan.setLimit(Integer.parseInt(m2.group(1))); } catch (Exception ignore) {}
            }
          }
        }
        if (queryLogicalPlan.getOffset() == null) {
          int o2 = upper2.indexOf(" OFFSET ");
          if (o2 >= 0) {
            String tailo2 = upper2.substring(o2+8).trim();
            java.util.regex.Matcher mo2 = java.util.regex.Pattern.compile("^([0-9]+)").matcher(tailo2);
            if (mo2.find()) {
              try { queryLogicalPlan.setOffset(Integer.parseInt(mo2.group(1))); } catch (Exception ignore) {}
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
  // Flatten subtree into tokens in appearance order for robust clause extraction
  private void flattenTokens(ParseTreeNode n, java.util.List<String> out) {
    String r = n.getRule();
    String lbl = n.getLabel();
    if (r != null && r.endsWith("_SYMBOL")) {
      if ("WHERE_SYMBOL".equals(r)) out.add("WHERE");
      else if ("GROUP_SYMBOL".equals(r)) out.add("GROUP");
      else if ("HAVING_SYMBOL".equals(r)) out.add("HAVING");
      else if ("ORDER_SYMBOL".equals(r)) out.add("ORDER");
      else if ("BY_SYMBOL".equals(r)) out.add("BY");
      else if ("LIMIT_SYMBOL".equals(r)) out.add("LIMIT");
      else if ("OFFSET_SYMBOL".equals(r)) out.add("OFFSET");
      else if ("AND_SYMBOL".equals(r)) out.add("AND");
      else if ("OR_SYMBOL".equals(r)) out.add("OR");
      else if ("NOT_SYMBOL".equals(r)) out.add("NOT");
      else if ("IS_SYMBOL".equals(r)) out.add("IS");
      else if ("LIKE_SYMBOL".equals(r)) out.add("LIKE");
      else if ("IN_SYMBOL".equals(r)) out.add("IN");
      else if ("BETWEEN_SYMBOL".equals(r)) out.add("BETWEEN");
      else if ("EQ_SYMBOL".equals(r)) out.add("=");
      else if ("NE_SYMBOL".equals(r)) out.add("!=");
      else if ("GT_SYMBOL".equals(r)) out.add(">");
      else if ("GTE_SYMBOL".equals(r)) out.add(">=");
      else if ("LT_SYMBOL".equals(r)) out.add("<");
      else if ("LTE_SYMBOL".equals(r)) out.add("<=");
      else if ("START_PAR_SYMBOL".equals(r)) out.add("(");
      else if ("CLOSE_PAR_SYMBOL".equals(r)) out.add(")");
      else if ("COMMA_SYMBOL".equals(r)) out.add(",");
      else if ("DOT_SYMBOL".equals(r)) out.add(".");
    } else if ("INT_LITERAL".equals(r) || "DECIMAL_LITERAL".equals(r) || "STRING_LITERAL".equals(r)) {
      if (lbl != null && !lbl.isEmpty()) out.add(lbl);
    } else if ("identifier".equals(r) || "LETTERS".equals(r) || "columnName".equals(r) || "qualifiedName".equals(r)) {
      if (lbl != null && !lbl.isEmpty()) out.add(lbl);
    }
    for (ParseTreeNode cnode : n.getChildren()) flattenTokens(cnode, out);
    }\r\n// Finalize WHERE/LIMIT/OFFSET from tokens if still missing
  private void finalizeClausesFromTokens(ParseTreeNode root) {
    java.util.List<String> toks = new java.util.ArrayList<>();
    flattenTokens(root, toks);
    // LIMIT
    if (this.queryLogicalPlan.getLimit() == null) {
      for (int i = 0; i < toks.size(); i++) {
        if ("LIMIT".equalsIgnoreCase(toks.get(i)) && i+1 < toks.size()) {
          try { this.queryLogicalPlan.setLimit(Integer.parseInt(toks.get(i+1).replaceAll("[^0-9]", ""))); } catch (Exception ignore) {}
          break;
        }
      }
    }
    // OFFSET
    if (this.queryLogicalPlan.getOffset() == null) {
      for (int i = 0; i < toks.size(); i++) {
        if ("OFFSET".equalsIgnoreCase(toks.get(i)) && i+1 < toks.size()) {
          try { this.queryLogicalPlan.setOffset(Integer.parseInt(toks.get(i+1).replaceAll("[^0-9]", ""))); } catch (Exception ignore) {}
          break;
        }
      }
    }
    // WHERE
    if (this.queryLogicalPlan.getWhereExpression() == null) {
      int w = -1, end = toks.size();
      for (int i = 0; i < toks.size(); i++) { if ("WHERE".equalsIgnoreCase(toks.get(i))) { w = i; break; } }
      if (w >= 0) {
        for (int i = w+1; i < toks.size(); i++) {
          String t = toks.get(i).toUpperCase(java.util.Locale.ROOT);
          if (t.equals("GROUP") || t.equals("HAVING") || t.equals("ORDER") || t.equals("LIMIT")) { end = i; break; }
        }
        if (end > w+1) {
          String where = String.join(" ", toks.subList(w+1, end)).trim();
          if (!where.isEmpty()) this.queryLogicalPlan.setWhereExpression(where);
        }
      }
}\r\n  // Finalize WHERE/LIMIT/OFFSET by scanning normalized text without relying on spaces
  private void finalizeClausesFromText(ParseTreeNode root) {
  String raw = extractText(root);
  if (raw == null) return;
  String sp = raw.trim().replaceAll("\\s+", " ");
  String Usp = sp.toUpperCase(java.util.Locale.ROOT);
  String norm = raw.toUpperCase(java.util.Locale.ROOT).replaceAll("\\s+", "");

  // WHERE: prefer spaced form, fallback to compact
  if (this.queryLogicalPlan.getWhereExpression() == null) {
    int w = Usp.indexOf(" WHERE ");
    if (w >= 0) {
      int end = Usp.length();
      for (String kw : new String[]{" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT "}) {
        int k = Usp.indexOf(kw, w+1);
        if (k >= 0 && k < end) end = k;
      }
      if (end > w+7) {
        String we = sp.substring(w+7, end).trim();
        if (!we.isEmpty()) this.queryLogicalPlan.setWhereExpression(we);
      }
    } else {
      int wn = norm.indexOf("WHERE");
      if (wn >= 0) {
        int endn = norm.length();
        for (String kw : new String[]{"GROUP","HAVING","ORDER","LIMIT"}) {
          int k = norm.indexOf(kw, wn+5);
          if (k >= 0 && k < endn) endn = k;
        }
        if (endn > wn+5) {
          String we = norm.substring(wn+5, endn);
          if (!we.isEmpty()) this.queryLogicalPlan.setWhereExpression(we);
        }
      }
    }
  }

  // ORDER BY: prefer spaced form, fallback to compact; support multi items
  if (this.queryLogicalPlan.getOrderBy() == null || this.queryLogicalPlan.getOrderBy().isEmpty()) {
    int ob = Usp.indexOf(" ORDER BY ");
    if (ob >= 0) {
      int endOb = Usp.length();
      for (String kw2 : new String[]{" LIMIT ", " HAVING ", " GROUP BY "}) {
        int k2 = Usp.indexOf(kw2, ob+1);
        if (k2 >= 0 && k2 < endOb) endOb = k2;
      }
      if (endOb > ob + 10) {
        String ordSeg = sp.substring(ob + 10, endOb).trim();
        String[] items = ordSeg.split(",");
        for (String it : items) {
          it = it.trim(); if (it.isEmpty()) continue;
          String[] toks = it.split("\\s+");
          String col = toks[0];
          boolean asc = true;
          if (toks.length >= 2 && toks[1].equalsIgnoreCase("DESC")) asc = false;
          com.dafei1288.jimsql.common.meta.JqColumn ccol = new com.dafei1288.jimsql.common.meta.JqColumn();
          ccol.setColumnName(stripQuotes(col).toLowerCase(java.util.Locale.ROOT));
          this.queryLogicalPlan.getOrderBy().add(new com.dafei1288.jimsql.server.plan.logical.OrderItem(ccol, asc));
        }
      }
    } else {
      int ob2 = norm.indexOf("ORDERBY");
      if (ob2 >= 0) {
        int end2 = norm.length();
        for (String kw : new String[]{"LIMIT","HAVING","GROUP"}) {
          int k = norm.indexOf(kw, ob2+7);
          if (k >= 0 && k < end2) end2 = k;
        }
        if (end2 > ob2 + 7) {
          String ord = norm.substring(ob2 + 7, end2);
          String[] parts = ord.split(",");
          for (String p : parts) {
            p = p.trim(); if (p.isEmpty()) continue;
            boolean asc = true; String col = p;
            if (p.endsWith("ASC")) { asc = true; col = p.substring(0, p.length()-3); }
            else if (p.endsWith("DESC")) { asc = false; col = p.substring(0, p.length()-4); }
            com.dafei1288.jimsql.common.meta.JqColumn ccol = new com.dafei1288.jimsql.common.meta.JqColumn();
            ccol.setColumnName(stripQuotes(col).toLowerCase(java.util.Locale.ROOT));
            this.queryLogicalPlan.getOrderBy().add(new com.dafei1288.jimsql.server.plan.logical.OrderItem(ccol, asc));
          }
        }
      }
    }
  }

  // LIMIT
  if (this.queryLogicalPlan.getLimit() == null) {
    java.util.regex.Matcher m = java.util.regex.Pattern.compile("LIMIT([0-9]+)").matcher(norm);
    if (m.find()) { try { this.queryLogicalPlan.setLimit(Integer.parseInt(m.group(1))); } catch (Exception ignore) {} }
  }
  // OFFSET
  if (this.queryLogicalPlan.getOffset() == null) {
    java.util.regex.Matcher m = java.util.regex.Pattern.compile("OFFSET([0-9]+)").matcher(norm);
    if (m.find()) { try { this.queryLogicalPlan.setOffset(Integer.parseInt(m.group(1))); } catch (Exception ignore) {} }
  }
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

}

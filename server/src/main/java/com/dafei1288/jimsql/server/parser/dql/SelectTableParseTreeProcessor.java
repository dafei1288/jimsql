package com.dafei1288.jimsql.server.parser.dql;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dafei1288.jimsql.server.plan.logical.LlmFunctionSpec;
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


  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SelectTableParseTreeProcessor.class);
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
    }
    finalizeAskLlmFromText(this.parseTree.getRoot());
    // Fallback: if selectItems not set by tree, parse from raw SELECT ... FROM segment
    try {
      java.util.List<com.dafei1288.jimsql.server.plan.logical.SelectItem> _sis = queryLogicalPlan.getSelectItems();
      if (_sis == null || _sis.isEmpty()) {
        String whole2 = extractText(this.parseTree.getRoot());
        if (whole2 != null) {
          java.util.regex.Pattern _p = java.util.regex.Pattern.compile("(?is)\\bselect\\s+(.*?)\\s+from\\b");
          java.util.regex.Matcher _m = _p.matcher(whole2);
          if (_m.find()) {
            String seg = _m.group(1).trim();
            java.util.List<String> parts = new java.util.ArrayList<>();
            int last = 0; int depth = 0; boolean inS = false; char q = 0;
            for (int i = 0; i < seg.length(); i++) {
              char c = seg.charAt(i);
              if (inS) { if (c == q) inS = false; continue; }
              if (c == '\'' || c == '"') { inS = true; q = c; continue; }
              if (c == '(') { depth++; continue; }
              if (c == ')') { if (depth > 0) depth--; continue; }
              if (c == ',' && depth == 0) { parts.add(seg.substring(last, i)); last = i + 1; }
            }
            parts.add(seg.substring(last));
            java.util.List<com.dafei1288.jimsql.server.plan.logical.SelectItem> out = new java.util.ArrayList<>();
            for (String it : parts) {
              String t = it.trim(); if (t.isEmpty()) continue;
              String col = t; String alias = null;
              String tl = t.toLowerCase(java.util.Locale.ROOT);
              int asIdx = tl.lastIndexOf(" as ");
              if (asIdx > 0) {
                col = t.substring(0, asIdx).trim();
                alias = t.substring(asIdx + 4).trim();
              } else {
                int lastWs = -1; inS = false; q = 0; depth = 0;
                for (int i = 0; i < t.length(); i++) {
                  char c = t.charAt(i);
                  if (inS) { if (c == q) inS = false; continue; }
                  if (c == '\'' || c == '"') { inS = true; q = c; continue; }
                  if (c == '(') { depth++; continue; }
                  if (c == ')') { if (depth > 0) depth--; continue; }
                  if (depth == 0 && Character.isWhitespace(c)) lastWs = i;
                }
                if (lastWs > 0 && lastWs < t.length() - 1) {
                  alias = t.substring(lastWs + 1).trim();
                  col = t.substring(0, lastWs).trim();
                }
              }
              if (alias != null && !alias.isEmpty()) {
                if ((alias.startsWith("") && alias.endsWith("")) || (alias.startsWith("\"") && alias.endsWith("\""))) {
                  alias = alias.substring(1, alias.length() - 1);
                }
              } else { alias = null; }
              com.dafei1288.jimsql.server.plan.logical.SelectItem si = new com.dafei1288.jimsql.server.plan.logical.SelectItem();
              si.setColumnName(col);
              si.setAlias(alias);
              out.add(si);
            }
            if (!out.isEmpty()) { queryLogicalPlan.setSelectItems(out); }
          }
        }
      }
    } catch (Throwable ignore) {}
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
              if (log.isDebugEnabled()) { log.debug("SELECT columns: {}", cols.stream().map(com.dafei1288.jimsql.common.meta.JqColumn::getColumnName).collect(java.util.stream.Collectors.joining(","))); }
              }
          }
      }


    // detect COUNT function
    if ("functionCall".equals(parseTreeNode.getRule())) {
      String fname = null;
      for (org.snt.inmemantlr.tree.ParseTreeNode ch : parseTreeNode.getChildren()) {
        if ("identifier".equals(ch.getRule())) { fname = stripQuotes(ch.getLabel()); break; }
      }
      if (fname != null && fname.equalsIgnoreCase("count")) {
        this.queryLogicalPlan.setCountStar(true);
      }
    }
      // detect aggregate functions in select list
    if ("functionCall".equals(parseTreeNode.getRule())) {
      String fname = null;
      String argCol = null; // null => COUNT(*) or COUNT(1)
      for (org.snt.inmemantlr.tree.ParseTreeNode ch : parseTreeNode.getChildren()) {
        String r = ch.getRule();
        if ("identifier".equals(r)) { fname = stripQuotes(ch.getLabel()); continue; }
        if ("qualifiedName".equals(r) || "columnName".equals(r) || "identifier".equals(r)) {
          argCol = stripQuotes(ch.getLabel());
        }
      }
      if (fname != null) {
        com.dafei1288.jimsql.server.plan.logical.AggregateSpec.Type t = null;
        if (fname.equalsIgnoreCase("count")) t = com.dafei1288.jimsql.server.plan.logical.AggregateSpec.Type.COUNT;
        else if (fname.equalsIgnoreCase("sum")) t = com.dafei1288.jimsql.server.plan.logical.AggregateSpec.Type.SUM;
        else if (fname.equalsIgnoreCase("avg")) t = com.dafei1288.jimsql.server.plan.logical.AggregateSpec.Type.AVG;
        else if (fname.equalsIgnoreCase("min")) t = com.dafei1288.jimsql.server.plan.logical.AggregateSpec.Type.MIN;
        else if (fname.equalsIgnoreCase("max")) t = com.dafei1288.jimsql.server.plan.logical.AggregateSpec.Type.MAX;
        if (t != null) {
          java.util.List<com.dafei1288.jimsql.server.plan.logical.AggregateSpec> aggs = this.queryLogicalPlan.getAggregates();
          aggs.add(new com.dafei1288.jimsql.server.plan.logical.AggregateSpec(t, argCol, null));
          this.queryLogicalPlan.setAggregates(aggs);
        }
      }
    }    // FROM first table
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
            queryLogicalPlan.setWhereExpression(extractExprText(parseTreeNode));
            whereNext = false;
        } else if (
                havingNext && queryLogicalPlan.getHavingExpression() == null) {
                    queryLogicalPlan.setHavingExpression(extractExprText(parseTreeNode));
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
          js.setOnExpression(extractExprText(ch));
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

    private String extractExprText(ParseTreeNode node) {
    java.util.List<String> toks = new java.util.ArrayList<>();
    flattenTokens(node, toks);
    java.util.List<String> norm = new java.util.ArrayList<>();
    for (String t : toks) { if (t != null) { String tt = t.trim(); if (!tt.isEmpty()) norm.add(tt); } }
    // if tokens already contain '=', return as-is (joined with spaces)
    for (String t : norm) { if ("=".equals(t)) { return String.join(" ", norm); } }
    // reconstruct two dotted identifiers like a.b and c.d
    java.util.LinkedHashSet<String> dotted = new java.util.LinkedHashSet<>();
    for (int i = 0; i + 2 < norm.size(); i++) {
      String a = norm.get(i), b = norm.get(i+1), c = norm.get(i+2);
      if (isIdentToken(a) && ".".equals(b) && isIdentToken(c)) {
        dotted.add(a + "." + c);
      }
    }
    for (String t : norm) { if (t.indexOf('.') >= 0) dotted.add(t.replace(" ", "")); }
    if (dotted.size() >= 2) {
      java.util.Iterator<String> it = dotted.iterator();
      String l = it.next(); String r = it.next();
      return l + " = " + r;
    }
    // fallback: pick first two identifier-like tokens
    java.util.List<String> ids = new java.util.ArrayList<>();
    for (String t : norm) {
      String u = t.toUpperCase(java.util.Locale.ROOT);
      if ("AND".equals(u) || "OR".equals(u) || ")".equals(t) || "(".equals(t) || ",".equals(t)) continue;
      ids.add(t);
    }
    if (ids.size() >= 2) return ids.get(0) + " = " + ids.get(1);
    return String.join(" ", norm);
  }
  private static boolean isIdentToken(String t) {
    if (t == null || t.isEmpty()) return false;
    for (int i = 0; i < t.length(); i++) {
      char ch = t.charAt(i);
      if (!(Character.isLetterOrDigit(ch) || ch == '_' || ch == '`' || ch == '"')) return false;
    }
    return true;
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
  }

  // Finalize WHERE/LIMIT/OFFSET from tokens if still missing
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
          try { this.queryLogicalPlan.setOffset(Integer.parseInt(toks.get(                                                                        i+1).replaceAll("[^0-9]", ""))); } catch (Exception ignore) {}
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
    }
  }
  // Finalize WHERE/LIMIT/OFFSET by scanning normalized text without relying on spaces
  private void finalizeClausesFromText(org.snt.inmemantlr.tree.ParseTreeNode root) {
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
        int k = Usp.indexOf(kw, w + 1);
        if (k >= 0 && k < end) end = k;
      }
      if (end > w + 7) {
        String we = sp.substring(w + 7, end).trim();
        if (!we.isEmpty()) this.queryLogicalPlan.setWhereExpression(we);
      }
    } else {
      int wn = norm.indexOf("WHERE");
      if (wn >= 0) {
        int endn = norm.length();
        for (String kw : new String[]{"GROUP", "HAVING", "ORDER", "LIMIT"}) {
          int k = norm.indexOf(kw, wn + 5);
          if (k >= 0 && k < endn) endn = k;
        }
        if (endn > wn + 5) {
          /* disabled uppercase WHERE fallback */
        }
      }
    }
  }

  // ORDER BY: prefer spaced form, fallback to compact; support multi items
  { this.queryLogicalPlan.getOrderBy().clear();
    int ob = Usp.indexOf(" ORDER BY ");
    if (ob >= 0) {
      int endOb = Usp.length();
      for (String kw2 : new String[]{" LIMIT ", " HAVING ", " GROUP BY ", " OFFSET "}) {
        int k2 = Usp.indexOf(kw2, ob + 1);
        if (k2 >= 0 && k2 < endOb) endOb = k2;
      }
      if (endOb > ob + 10) {
        String ordSeg = sp.substring(ob + 10, endOb).trim();
        String[] items = ordSeg.split(",");
        for (String it : items) {
          it = it.trim();
          if (it.isEmpty()) continue;
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
        for (String kw : new String[]{"LIMIT", "HAVING", "GROUP", "OFFSET"}) {
          int k = norm.indexOf(kw, ob2 + 7);
          if (k >= 0 && k < end2) end2 = k;
        }
        if (end2 > ob2 + 7) {
          String ord = norm.substring(ob2 + 7, end2);
          String[] parts = ord.split(",");
          for (String p : parts) {
            p = p.trim(); if (p.isEmpty()) continue;
            boolean asc = true; String col = p;
            if (p.endsWith("ASC")) { asc = true; col = p.substring(0, p.length() - 3); }
            else if (p.endsWith("DESC")) { asc = false; col = p.substring(0, p.length() - 4); }
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


  private void finalizeAskLlmFromText(org.snt.inmemantlr.tree.ParseTreeNode root) {
    try {
      String raw = extractText(root);
      if (raw == null) return;
      String s = raw;
      int idx = indexOfIgnoreCase(s, "ask_llm(");
      if (idx < 0) return;
      int start = idx + "ask_llm(".length();
      int end = findArgsEnd(s, start);
      if (end < 0) return;
      String args = s.substring(start, end);

      String prompt = extractFirstQuoted(args);
      if (prompt == null) return;

      LlmFunctionSpec spec = new LlmFunctionSpec(prompt);

      java.util.List<String> parts = splitTopLevel(args, ',');
      for (int i = 1; i < parts.size(); i++) {
        String p = parts.get(i).trim();
        if (p.isEmpty()) continue;
        int eq = p.indexOf('=');
        if (eq <= 0) continue;
        String k = p.substring(0, eq).trim().toLowerCase(java.util.Locale.ROOT);
        String v = unquote(p.substring(eq + 1).trim());
        if ("label".equals(k)) spec.setLabel(v);
        else if ("model".equals(k)) spec.setModel(v);
        else if ("base_url".equals(k)) spec.setBaseUrl(v);
        else if ("api_key".equals(k)) spec.setApiKey(v);
        else if ("api_type".equals(k)) spec.setApiType(v);
        else if ("temperature".equals(k)) spec.setTemperature(v);
        else if ("stream".equals(k)) spec.setStream(v);
        else if ("thinking".equals(k)) spec.setThinking(v);
      }

      if (log.isDebugEnabled()) {
        StringBuilder keys = new StringBuilder();
        if (spec.getModel() != null) keys.append("model,");
        if (spec.getBaseUrl() != null) keys.append("base_url,");
        if (spec.getApiKey() != null) keys.append("api_key,");
        if (spec.getApiType() != null) keys.append("api_type,");
        if (spec.getTemperature() != null) keys.append("temperature,");
        if (spec.getStream() != null) keys.append("stream,");
        if (spec.getThinking() != null) keys.append("thinking,");
        if (keys.length() > 0) keys.setLength(keys.length() - 1);
        log.debug("ASK_LLM parsed: label={}, prompt.len={}, overrides=[{}]",
            spec.getLabel(),
            spec.getPrompt() == null ? 0 : spec.getPrompt().length(),
            keys.toString());
      }
      queryLogicalPlan.setLlmFunctionSpec(spec);
    } catch (Throwable ignore) {}
  }

  private static int indexOfIgnoreCase(String s, String pat) {
    for (int i = 0; i + pat.length() <= s.length(); i++) {
      if (s.regionMatches(true, i, pat, 0, pat.length())) return i;
    }
    return -1;
  }

  private static int findArgsEnd(String s, int from) {
    boolean inS = false; char q = 0; int depth = 0;
    for (int i = from; i < s.length(); i++) {
      char c = s.charAt(i);
      if (inS) { if (c == q) inS = false; continue; }
      if (c == '\'' || c == '"') { inS = true; q = c; continue; }
      if (c == '(') { depth++; continue; }
      if (c == ')') { if (depth == 0) return i; depth--; continue; }
    }
    return -1;
  }

  private static java.util.List<String> splitTopLevel(String s, char sep) {
    java.util.List<String> out = new java.util.ArrayList<>();
    boolean inS = false; char q = 0; int last = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (inS) { if (c == q) inS = false; continue; }
      if (c == '\'' || c == '"') { inS = true; q = c; continue; }
      if (c == sep) { out.add(s.substring(last, i)); last = i + 1; }
    }
    out.add(s.substring(last));
    return out;
  }

  private static String extractFirstQuoted(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\'' || c == '"') {
        char q = c; int j = i + 1;
        while (j < s.length() && s.charAt(j) != q) j++;
        if (j < s.length()) return s.substring(i + 1, j);
        break;
      }
    }
    return null;
  }

  private static String unquote(String v) {
    if (v == null) return null;
    v = v.trim();
    if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith("\"") && v.endsWith("\""))) {
      return v.substring(1, v.length() - 1);
    }
    return v;
  }  private void collectSelectColumns(org.snt.inmemantlr.tree.ParseTreeNode node, java.util.List<com.dafei1288.jimsql.common.meta.JqColumn> out) {  if ("columnName".equals(node.getRule())) {
    com.dafei1288.jimsql.common.meta.JqColumn c = new com.dafei1288.jimsql.common.meta.JqColumn();
    c.setColumnName(stripQuotes(node.getLabel()));
    out.add(c);
    return;
  }
  if ("qualifiedName".equals(node.getRule())) {
    java.util.List<String> idents = new java.util.ArrayList<>();
    for (org.snt.inmemantlr.tree.ParseTreeNode ch : node.getChildren()) {
      if ("identifier".equals(ch.getRule())) {
        idents.add(stripQuotes(ch.getLabel()));
      }
    }
    if (!idents.isEmpty()) {
      String qn = String.join(".", idents);
      com.dafei1288.jimsql.common.meta.JqColumn c = new com.dafei1288.jimsql.common.meta.JqColumn();
      c.setColumnName(qn);
      out.add(c);
      return;
    }
  }
  for (org.snt.inmemantlr.tree.ParseTreeNode ch : node.getChildren()) {
    collectSelectColumns(ch, out);
  }}

}






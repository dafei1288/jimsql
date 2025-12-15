package com.dafei1288.jimsql.server.parser.dml;

import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.SqlStatementEnum;
import com.dafei1288.jimsql.server.plan.logical.DeleteLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.UpdateLogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.InsertLogicalPlan;
import java.util.LinkedHashMap;
import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;

// Parse UPDATE/DELETE and produce minimal logical plans
public class DmlScriptParseTreeProcessor extends ScriptParseTreeProcessor {

  private UpdateLogicalPlan update;
  private DeleteLogicalPlan del;
  private InsertLogicalPlan insert;

  public DmlScriptParseTreeProcessor(ParseTree parseTree) { super(parseTree); }

  @Override
  public Object getResult() { return (update != null) ? update : (insert != null ? insert : del); }

  @Override
  protected void process(ParseTreeNode n) throws ParseTreeProcessorException {
    // map subtree for extractText support
    if (smap.get(n) == null) {
      ParseTree sub = this.parseTree.getSubtrees(it -> it == n).stream().findFirst().orElse(null);
      if (sub == null) sub = this.parseTree.getSubtrees(it -> it.getRule().equals(n.getRule())).stream().findFirst().orElse(null);
      if (sub != null) smap.put(n, sub);
    }

    String r = n.getRule();
    if ("updateTable".equals(r)) { handleUpdate(n); }
    if ("deleteTable".equals(r)) { handleDelete(n); }
      if ("insertTable".equals(r)) { handleInsert(n); }`r`n}

  private void handleUpdate(ParseTreeNode n) {
    UpdateLogicalPlan plan = new UpdateLogicalPlan();
    JqTable t = new JqTable();
    // table name
    for (ParseTreeNode ch : n.getChildren()) {
      if ("tableName".equals(ch.getRule())) { t.setTableName(stripQuotes(ch.getLabel())); break; }
    }
    plan.setTable(t);

    // SET assignments
    LinkedHashMap<String,String> sets = new LinkedHashMap<>();
    for (ParseTreeNode ch : n.getChildren()) {
      if ("updateList".equals(ch.getRule())) {
        for (ParseTreeNode it : ch.getChildren()) {
          if ("updateItem".equals(it.getRule())) {
            String col = null; String val = null; ParseTreeNode exprNode = null;
            for (ParseTreeNode leaf : it.getChildren()) {
              if ("columnName".equals(leaf.getRule())) col = stripQuotes(leaf.getLabel());
              else if ("expr".equals(leaf.getRule())) exprNode = leaf;
            }
            if (exprNode != null) val = normalizeExprText(extractText(exprNode));
            if (col != null && val != null) sets.put(col, val);
          }
        }
      }
    }
    plan.setAssignments(sets);

    // WHERE expression (raw text)
    for (ParseTreeNode ch : n.getChildren()) {
      if ("expression".equals(ch.getRule())) {
        String w = extractText(ch);
        if (w != null && !w.trim().isEmpty()) plan.setWhereExpression(w.trim());
        break;
      }
    }

    this.update = plan;
    this.setSqlStatementEnum(SqlStatementEnum.UPDATE_TABLE);
    this.setCurrentParseTreeProcessor(this);
  }

  private void handleDelete(ParseTreeNode n) {
    DeleteLogicalPlan plan = new DeleteLogicalPlan();
    JqTable t = new JqTable();
    for (ParseTreeNode ch : n.getChildren()) {
      if ("tableName".equals(ch.getRule())) { t.setTableName(stripQuotes(ch.getLabel())); break; }
    }
    plan.setTable(t);
    for (ParseTreeNode ch : n.getChildren()) {
      if ("expression".equals(ch.getRule())) {
        String w = extractText(ch);
        if (w != null && !w.trim().isEmpty()) plan.setWhereExpression(w.trim());
        break;
      }
    }
    this.del = plan;
    this.setSqlStatementEnum(SqlStatementEnum.DELETE_TABLE);
    this.setCurrentParseTreeProcessor(this);
  }

  private String stripQuotes(String s) {
    if (s == null || s.length() < 2) return s;
    char f = s.charAt(0), l = s.charAt(s.length()-1);
    if ((f == '`' && l == '`') || (f == '"' && l == '"')) return s.substring(1, s.length()-1);
    return s;
  }

  private String extractText(ParseTreeNode node) {
    StringBuilder sb = new StringBuilder();
    dfs(node, sb);
    return sb.toString().trim().replaceAll("\\s+", " ");
  }

  private void dfs(ParseTreeNode n, StringBuilder sb) {
    String lbl = n.getLabel();
    if (lbl == null || lbl.isEmpty()) {
      String r = n.getRule();
      if (r != null && r.endsWith("_SYMBOL")) lbl = r.substring(0, r.length() - "_SYMBOL".length());
    }
    if (lbl != null && !lbl.isEmpty()) {
      if (sb.length() > 0) sb.append(' ');
      sb.append(lbl);
    }
    for (ParseTreeNode c : n.getChildren()) dfs(c, sb);
  }

  private String normalizeExprText(String v) {
    if (v == null) return null;
    String s = v.trim();
    if (s.length() >= 2 && s.charAt(0)=='\'' && s.charAt(s.length()-1)=='\'') return s.substring(1, s.length()-1);
    return s;
  }
  private void handleInsert(ParseTreeNode n) {
    InsertLogicalPlan plan = new InsertLogicalPlan();
    com.dafei1288.jimsql.common.meta.JqTable t = new com.dafei1288.jimsql.common.meta.JqTable();
    // table name
    for (ParseTreeNode ch : n.getChildren()) {
      if ("tableName".equals(ch.getRule())) { t.setTableName(stripQuotes(ch.getLabel())); break; }
    }
    plan.setTable(t);
    // optional fields list
    for (ParseTreeNode ch : n.getChildren()) {
      if ("fields".equals(ch.getRule())) {
        java.util.List<String> cols = new java.util.ArrayList<>();
        for (ParseTreeNode it : ch.getChildren()) {
          if ("insertIdentifier".equals(it.getRule()) || "identifier".equals(it.getRule())) {
            cols.add(stripQuotes(it.getLabel()));
          }
        }
        plan.setColumns(cols);
      }
    }
    // VALUES (...) , (...)
    for (ParseTreeNode ch : n.getChildren()) {
      if ("insertValues".equals(ch.getRule())) {
        for (ParseTreeNode it : ch.getChildren()) {
          if ("valueList".equals(it.getRule())) {
            for (ParseTreeNode vgrp : it.getChildren()) {
              if ("values".equals(vgrp.getRule())) {
                java.util.List<String> row = new java.util.ArrayList<>();
                for (ParseTreeNode expr : vgrp.getChildren()) {
                  if ("expr".equals(expr.getRule())) {
                    String val = normalizeInsertExpr(extractText(expr));
                    row.add(val);
                  }
                }
                plan.getRows().add(row);
              }
            }
          }
        }
      }
    }
    this.insert = plan;
    this.setSqlStatementEnum(SqlStatementEnum.INSERT_TABLE);
    this.setCurrentParseTreeProcessor(this);
  }

  private String normalizeInsertExpr(String v) {
    if (v == null) return "";
    String s = v.trim();
    // NULL literal => empty cell in CSV backend
    if (s.equalsIgnoreCase("NULL")) return "";
    // unquote single-quoted string; keep bare numbers/identifiers as-is
    if (s.length() >= 2 && s.charAt(0)=='\'' && s.charAt(s.length()-1)=='\'') return s.substring(1, s.length()-1);
    return s;
  }
}

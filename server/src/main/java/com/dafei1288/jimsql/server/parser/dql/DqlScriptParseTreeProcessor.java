package com.dafei1288.jimsql.server.parser.dql;

import com.dafei1288.jimsql.server.parser.ScriptParseTreeProcessor;
import com.dafei1288.jimsql.server.parser.SqlStatementEnum;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import org.snt.inmemantlr.exceptions.ParseTreeProcessorException;
import org.snt.inmemantlr.tree.ParseTree;
import org.snt.inmemantlr.tree.ParseTreeNode;
import org.snt.inmemantlr.tree.ParseTreeProcessor;

public class DqlScriptParseTreeProcessor extends ScriptParseTreeProcessor {

  public DqlScriptParseTreeProcessor(ParseTree parseTree) {
    super(parseTree);
  }

  @Override
  public Object getResult() {
    if (this.getCurrentParseTreeProcessor() != null) {
      Object r = this.getCurrentParseTreeProcessor().getResult();
      if (r instanceof QueryLogicalPlan) {
        finalizeClausesFromTokensOn((QueryLogicalPlan) r, this.parseTree.getRoot());
      }
      return r;
    }
    // Prefer: locate selectTable subtree
    for (ParseTreeNode n : this.parseTree.getNodes()) {
      if ("selectTable".equals(n.getRule())) {
        ParseTree sub = (ParseTree) smap.get(n);
        if (sub != null) {
          SelectTableParseTreeProcessor p = new SelectTableParseTreeProcessor(sub);
          this.setCurrentParseTreeProcessor(p);
          this.setSqlStatementEnum(SqlStatementEnum.SELECT_TABLE);
          try { p.process(); } catch (Exception ignore) {}
          QueryLogicalPlan q = p.getResult();
          finalizeClausesFromTokensOn(q, this.parseTree.getRoot());
          return q;
        }
      }
    }
    // Fallback: run over entire DQL subtree
    try {
      SelectTableParseTreeProcessor p = new SelectTableParseTreeProcessor(this.parseTree);
      this.setCurrentParseTreeProcessor(p);
      this.setSqlStatementEnum(SqlStatementEnum.SELECT_TABLE);
      p.process();
      QueryLogicalPlan q = p.getResult();
      finalizeClausesFromTokensOn(q, this.parseTree.getRoot());
      return q;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected void process(ParseTreeNode parseTreeNode) throws ParseTreeProcessorException {
    if ("selectTable".equals(parseTreeNode.getRule())) {
      SelectTableParseTreeProcessor processor = new SelectTableParseTreeProcessor((ParseTree) smap.get(parseTreeNode));
      this.setCurrentParseTreeProcessor(processor);
      this.setSqlStatementEnum(SqlStatementEnum.SELECT_TABLE);
      this.getCurrentParseTreeProcessor().process();
    }
  }

  private void flattenTokens(org.snt.inmemantlr.tree.ParseTreeNode n, java.util.List<String> out) {
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
      else if ("EQ_SYMBOL".equals(r)) out.add("=");
      else if ("NE_SYMBOL".equals(r)) out.add("!=");
      else if ("GT_SYMBOL".equals(r)) out.add(">");
      else if ("GTE_SYMBOL".equals(r)) out.add(">=");
      else if ("LT_SYMBOL".equals(r)) out.add("<");
      else if ("LTE_SYMBOL".equals(r)) out.add("<=");
      else if ("START_PAR_SYMBOL".equals(r)) out.add("(");
      else if ("CLOSE_PAR_SYMBOL".equals(r)) out.add(")");
      else if ("COMMA_SYMBOL".equals(r)) out.add(",");
    } else if ("INT_LITERAL".equals(r) || "DECIMAL_LITERAL".equals(r) || "STRING_LITERAL".equals(r)) {
      if (lbl != null && !lbl.isEmpty()) out.add(lbl);
    } else if ("identifier".equals(r) || "LETTERS".equals(r) || "columnName".equals(r) || "qualifiedName".equals(r)) {
      if (lbl != null && !lbl.isEmpty()) out.add(lbl);
    }
    for (org.snt.inmemantlr.tree.ParseTreeNode c : n.getChildren()) flattenTokens(c, out);
  }

  private void finalizeClausesFromTokensOn(QueryLogicalPlan ql, org.snt.inmemantlr.tree.ParseTreeNode root) {
    if (ql == null || root == null) return;
    java.util.List<String> toks = new java.util.ArrayList<>();
    flattenTokens(root, toks);
    if (ql.getLimit() == null) {
      for (int i = 0; i < toks.size(); i++) {
        if ("LIMIT".equalsIgnoreCase(toks.get(i)) && i+1 < toks.size()) {
          try { ql.setLimit(Integer.parseInt(toks.get(i+1).replaceAll("[^0-9]", ""))); } catch (Exception ignore) {}
          break;
        }
      }
    }
    if (ql.getOffset() == null) {
      for (int i = 0; i < toks.size(); i++) {
        if ("OFFSET".equalsIgnoreCase(toks.get(i)) && i+1 < toks.size()) {
          try { ql.setOffset(Integer.parseInt(toks.get(i+1).replaceAll("[^0-9]", ""))); } catch (Exception ignore) {}
          break;
        }
      }
    }
    if (ql.getWhereExpression() == null) {
      int w = -1, end = toks.size();
      for (int i = 0; i < toks.size(); i++) { if ("WHERE".equalsIgnoreCase(toks.get(i))) { w = i; break; } }
      if (w >= 0) {
        for (int i = w+1; i < toks.size(); i++) {
          String t = toks.get(i).toUpperCase(java.util.Locale.ROOT);
          if (t.equals("GROUP") || t.equals("HAVING") || t.equals("ORDER") || t.equals("LIMIT")) { end = i; break; }
        }
        if (end > w+1) {
          String where = String.join(" ", toks.subList(w+1, end)).trim();
          if (!where.isEmpty()) ql.setWhereExpression(where);
        }
      }
    }
  }
}
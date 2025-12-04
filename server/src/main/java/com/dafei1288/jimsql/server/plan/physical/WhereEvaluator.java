package com.dafei1288.jimsql.server.plan.physical;

import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqTable;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Minimal boolean expression parser/evaluator for WHERE:
 * - Supports AND/OR with parentheses and NOT
 * - Comparisons: = != > >= < <= (numeric vs string based on table metadata)
 * - IS NULL / IS NOT NULL (empty string is treated as NULL)
 * - LIKE (%, _) and NOT LIKE
 * - IN (...) and NOT IN (...), numbers or quoted strings
 *
 * Input identifiers may be quoted/backticked or qualified (a.b) - we normalize to column name.
 */
final class WhereEvaluator {

    static Node parse(String where) {
        Tokenizer tz = new Tokenizer(where);
        Parser p = new Parser(tz);
        return p.parseExpression();
    }

    interface Node {
        boolean eval(Map<String,String> row, JqTable jt);
        default void collectColumns(Set<String> out) {}
    }

    static final class Bin implements Node {
        final String op; // AND / OR
        final Node left, right;
        Bin(String op, Node l, Node r) { this.op = op; this.left = l; this.right = r; }
        public boolean eval(Map<String,String> row, JqTable jt) {
            if ("AND".equals(op)) return left.eval(row, jt) && right.eval(row, jt);
            return left.eval(row, jt) || right.eval(row, jt);
        }
        public void collectColumns(Set<String> out) { left.collectColumns(out); right.collectColumns(out); }
    }

    static final class Not implements Node {
        final Node inner;
        Not(Node n) { this.inner = n; }
        public boolean eval(Map<String,String> row, JqTable jt) { return !inner.eval(row, jt); }
        public void collectColumns(Set<String> out) { inner.collectColumns(out); }
    }

    static final class Cmp implements Node {
        final String col;
        final String op;
        final String litStr;
        final BigDecimal litNum;
        Cmp(String c, String o, String s, BigDecimal n) { this.col=c; this.op=o; this.litStr=s; this.litNum=n; }
        public boolean eval(Map<String,String> row, JqTable jt) {
            String key = normalize(col);
            String raw = getCaseInsensitive(row, key);
            if (raw == null) raw = "";
            int t = columnSqlType(jt, key);
            if (isNumericType(t) && litNum != null) {
                try {
                    BigDecimal left = new BigDecimal(raw.trim());
                    int c = left.compareTo(litNum);
                    return cmpSwitch(c, op);
                } catch (Exception ignore) {}
            }
            int c = raw.compareTo(litStr);
            return cmpSwitch(c, op);
        }
        public void collectColumns(Set<String> out) { out.add(normalize(col)); }
    }

    static final class IsNull implements Node {
        final String col; final boolean negate;
        IsNull(String c, boolean n) { this.col=c; this.negate=n; }
        public boolean eval(Map<String,String> row, JqTable jt) {
            String v = getCaseInsensitive(row, normalize(col));
            boolean isNull = (v == null || v.isEmpty());
            return negate ? !isNull : isNull;
        }
        public void collectColumns(Set<String> out) { out.add(normalize(col)); }
    }

    static final class Like implements Node {
        final String col; final Pattern pat; final boolean negate;
        Like(String c, String pattern, boolean n) { this.col=c; this.negate=n; this.pat = Pattern.compile(sqlLikeToRegex(pattern), Pattern.DOTALL); }
        public boolean eval(Map<String,String> row, JqTable jt) {
            String v = getCaseInsensitive(row, normalize(col));
            boolean m = v != null && pat.matcher(v).matches();
            return negate ? !m : m;
        }
        public void collectColumns(Set<String> out) { out.add(normalize(col)); }
    }

    static final class InList implements Node {
        final String col; final List<String> lits; final List<BigDecimal> nums; final boolean negate;
        InList(String c, List<String> s, List<BigDecimal> n, boolean neg) { this.col=c; this.lits=s; this.nums=n; this.negate=neg; }
        public boolean eval(Map<String,String> row, JqTable jt) {
            String key = normalize(col);
            String v = getCaseInsensitive(row, key);
            if (v == null) v = "";
            int t = columnSqlType(jt, key);
            boolean hit;
            if (isNumericType(t) && nums != null && !nums.isEmpty()) {
                try {
                    BigDecimal left = new BigDecimal(v.trim());
                    hit = { boolean h=false; for (java.math.BigDecimal _n : nums) { if (left.compareTo(_n)==0) { h=true; break; } } hit = h; }
                } catch (Exception ex) { hit = false; }
            } else {
                { boolean h=false; if (lits!=null) { for (String _s : lits) { if (_s.equals(v)) { h=true; break; } } } hit = h; }
            }
            return negate ? !hit : hit;
        }
        public void collectColumns(Set<String> out) { out.add(normalize(col)); }
    }

    // ---------------- Parser ----------------

    static final class Parser {
        final Tokenizer tz;
        Parser(Tokenizer t) { this.tz = t; }
        Node parseExpression() {
            Node n = parseOr();
            if (tz.peek().type != TokType.EOF) { /* ignore tail */ }
            return n==null? (row,jt)->true : n;
        }
        Node parseOr() {
            Node left = parseAnd();
            while (tz.peekIsWord("OR")) { tz.next(); Node right = parseAnd(); left = new Bin("AND".equals("OR")?"AND":"OR", left, right); }
            return left;
        }
        Node parseAnd() {
            Node left = parseNot();
            while (tz.peekIsWord("AND")) { tz.next(); Node right = parseNot(); left = new Bin("AND", left, right); }
            return left;
        }
        Node parseNot() {
            if (tz.peekIsWord("NOT")) { tz.next(); return new Not(parsePrimary()); }
            return parsePrimary();
        }
        Node parsePrimary() {
            if (tz.peek().type == TokType.LPAREN) { tz.next(); Node n = parseOr(); tz.expect(TokType.RPAREN); return n; }
            // expect identifier
            Token id = tz.next(); if (id.type != TokType.IDENT) return null;
            // IS [NOT] NULL
            if (tz.peekIsWord("IS")) {
                tz.next(); boolean neg = false; if (tz.peekIsWord("NOT")) { tz.next(); neg = true; }
                if (tz.peekIsWord("NULL")) { tz.next(); return new IsNull(id.text, neg); }
                return null;
            }
            // [NOT] LIKE
            boolean neg = false;
            if (tz.peekIsWord("NOT")) { tz.next(); neg = true; }
            if (tz.peekIsWord("LIKE")) {
                tz.next(); Token lit = tz.next(); String p = tokenToString(lit); return new Like(id.text, p, neg);
            }
            // [NOT] IN (...)
            if (tz.peekIsWord("IN") || (neg && tz.peekIsWord("IN"))) {
                if (!tz.peekIsWord("IN")) return null; tz.next(); tz.expect(TokType.LPAREN);
                List<String> ss = new ArrayList<>(); List<BigDecimal> ns = new ArrayList<>();
                boolean numericOnly = true;
                while (tz.peek().type != TokType.RPAREN && tz.peek().type != TokType.EOF) {
                    Token v = tz.next();
                    if (v.type == TokType.COMMA) continue;
                    if (v.type == TokType.STRING) { ss.add(unquote(v.text)); numericOnly = false; }
                    else if (v.type == TokType.NUMBER) { try { ns.add(new BigDecimal(v.text)); } catch (Exception ignore) {} }
                    else { ss.add(v.text); numericOnly = false; }
                }
                tz.expect(TokType.RPAREN);
                if (!numericOnly && ns.isEmpty()) ns = null;
                if (!numericOnly && ss.isEmpty()) ss = null;
                return new InList(id.text, ss, ns, neg);
            }
            // comparison
            Token op = tz.next();
            if (op.type != TokType.OP) return null;
            Token lit = tz.next(); String s = tokenToString(lit); BigDecimal n = tokenToNumber(lit);
            return new Cmp(id.text, op.text, s, n);
        }
    }

    enum TokType { IDENT, STRING, NUMBER, OP, LPAREN, RPAREN, COMMA, WORD, EOF }
    static final class Token { final TokType type; final String text; Token(TokType t, String s) { type=t; text=s; } }

    static final class Tokenizer {
        final String s; int i=0; Token lookahead;
        Tokenizer(String s) { this.s = (s==null?"":s).trim(); }
        Token peek() { if (lookahead==null) lookahead = nextToken(); return lookahead; }
        boolean peekIsWord(String w) { Token t = peek(); return t.type==TokType.WORD && t.text.equalsIgnoreCase(w); }
        Token next() { Token t = (lookahead!=null? lookahead : nextToken()); lookahead = null; return t; }
        void expect(TokType tt) { Token t = next(); /* ignore mismatches for robustness */ }
        Token nextToken() {
            // skip ws
            int n = s.length();
            while (i<n) { char c = s.charAt(i); if (c==' '||c=='\t'||c=='\r'||c=='\n') i++; else break; }
            if (i>=n) return new Token(TokType.EOF, "");
            char c = s.charAt(i);
            if (c=='(') { i++; return new Token(TokType.LPAREN, "("); }
            if (c==')') { i++; return new Token(TokType.RPAREN, ")"); }
            if (c==',') { i++; return new Token(TokType.COMMA, ","); }
            // string literal
            if (c=='\'') { int j=i+1; StringBuilder sb=new StringBuilder(); boolean esc=false; for (; j<n; j++) { char ch=s.charAt(j); if (esc) { sb.append(ch); esc=false; continue;} if (ch=='\\') { esc=true; continue;} if (ch=='\'') { j++; break;} sb.append(ch);} String str=sb.toString(); i=j; return new Token(TokType.STRING, "'"+str+"'"); }
            // number
            if ((c>='0'&&c<='9') || (c=='-' && i+1<n && Character.isDigit(s.charAt(i+1)))) {
                int j=i+1; boolean dot=false; while (j<n) { char ch=s.charAt(j); if (Character.isDigit(ch)) { j++; continue;} if (ch=='.' && !dot) { dot=true; j++; continue;} break; }
                String num=s.substring(i,j); i=j; return new Token(TokType.NUMBER, num);
            }
            // operators: >= <= != = > <
            if (i+1<n) {
                String two=s.substring(i,i+2);
                if (two.equals(">=")||two.equals("<=")||two.equals("!=")) { i+=2; return new Token(TokType.OP, two);} }
            if (c=='='||c=='>'||c=='<') { i++; return new Token(TokType.OP, String.valueOf(c)); }
            // word/ident
            int j=i; while (j<n) { char ch=s.charAt(j); if (Character.isLetterOrDigit(ch) || ch=='_' || ch=='.' || ch=='`' || ch=='"') { j++; } else break; }
            String w = s.substring(i,j); i=j;
            // classify as keyword
            String wl = w.toLowerCase(java.util.Locale.ROOT);
            if (wl.equals("and")||wl.equals("or")||wl.equals("not")||wl.equals("is")||wl.equals("null")||wl.equals("like")||wl.equals("in")) return new Token(TokType.WORD, w);
            return new Token(TokType.IDENT, w);
        }
    }

    // ---------------- utils ----------------
    static String tokenToString(Token t) { if (t.type==TokType.STRING) return unquote(t.text); return t.text; }
    static BigDecimal tokenToNumber(Token t) { try { if (t.type==TokType.NUMBER) return new BigDecimal(t.text); } catch (Exception e) {} return null; }
    static String unquote(String s) { if (s==null) return null; if (s.length()>=2 && s.charAt(0)=='\'' && s.charAt(s.length()-1)=='\'') return s.substring(1, s.length()-1); return s; }
    static String normalize(String c) { if (c==null) return null; c=stripQuotes(c); int dot=c.lastIndexOf('.'); if (dot>=0) c=c.substring(dot+1); return c; }
    static String stripQuotes(String s){ if (s==null||s.length()<2) return s; char f=s.charAt(0), l=s.charAt(s.length()-1); if ((f=='`'&&l=='`')||(f=='"'&&l=='"')) return s.substring(1,s.length()-1); return s; }
    static boolean cmpSwitch(int cmp, String op){ if ("=".equals(op)) return cmp==0; if ("!=".equals(op)) return cmp!=0; if (">".equals(op)) return cmp>0; if (">=".equals(op)) return cmp>=0; if ("<".equals(op)) return cmp<0; if ("<=".equals(op)) return cmp<=0; return false; }
    static boolean isNumericType(int t){ switch (t){ case java.sql.Types.INTEGER: case java.sql.Types.BIGINT: case java.sql.Types.SMALLINT: case java.sql.Types.TINYINT: case java.sql.Types.DOUBLE: case java.sql.Types.FLOAT: case java.sql.Types.DECIMAL: case java.sql.Types.NUMERIC: return true; default: return false; } }
    static int columnSqlType(JqTable jt, String col){ for (String k : jt.getJqTableLinkedHashMap().keySet()) { if (k.equalsIgnoreCase(col)) { JqColumn jc = jt.getJqTableLinkedHashMap().get(k); if (jc != null) return jc.getColumnType(); } } return java.sql.Types.VARCHAR; }
    static String getCaseInsensitive(Map<String,String> row, String col){ if (row==null||col==null) return null; for (String k : row.keySet()) { if (k.equalsIgnoreCase(col)) return row.get(k);} return null; }
    static String sqlLikeToRegex(String p){ StringBuilder sb=new StringBuilder(); for (int i=0;i<p.length();i++){ char c=p.charAt(i); switch(c){ case '%': sb.append(".*"); break; case '_': sb.append('.'); break; case '.': case '[': case ']': case '(': case ')': case '{': case '}': case '^': case '$': case '|': case '+': case '*': case '?': case '\\': sb.append('\\').append(c); break; default: sb.append(c);} } return "^"+sb+"$"; }

    static Set<String> referencedColumns(Node n){ Set<String> s=new LinkedHashSet<>(); if (n!=null) n.collectColumns(s); return s; }
}
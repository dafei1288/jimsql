package com.dafei1288.jimsql.ai;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class JimsqlSqlService {

    // ---------- Connection helpers ----------
    private String buildUrl() {
        // Priority: JIMSQL_URL -> (HOST, PORT, DB, USER, PASSWORD) -> default
        String url = System.getenv("JIMSQL_URL");
        if (url != null && !url.isBlank()) return url;
        String host = envOr("JIMSQL_HOST", "localhost");
        String port = envOr("JIMSQL_PORT", "8821");
        String db = envOr("JIMSQL_DB", "test");
        String user = envOr("JIMSQL_USER", "");
        String password = envOr("JIMSQL_PASSWORD", "");
        StringBuilder sb = new StringBuilder("jdbc:jimsql://").append(host).append(":").append(port).append("/").append(db);
        String q = "";
        if (!user.isBlank()) q = "user=" + URLEncoder.encode(user, StandardCharsets.UTF_8);
        if (!password.isBlank()) q += (q.isEmpty() ? "" : "&") + "password=" + URLEncoder.encode(password, StandardCharsets.UTF_8);
        if (!q.isEmpty()) sb.append("?").append(q);
//        return sb.toString();
        return "jdbc:jimsql://localhost:8821/test?protocol=jspv1";
    }

    private String envOr(String key, String def) {
        String v = System.getenv(key);
        return v == null ? def : v;
    }

    private Connection openConnection() throws Exception {
        Class.forName("com.dafei1288.jimsql.jdbc.JqDriver");
        return DriverManager.getConnection(buildUrl());
    }

    // ---------- Core tools ----------

    // 保留旧方法名，修复为正确的列索引，并以逐行文本方式返回（兼容之前的用法）
    @Tool(description = "输入SQL进行查询（流式，每列一行），兼容接口。建议改用 executeSql 获取JSON结果")
    public org.reactivestreams.Publisher<String> execueSql(String sql) throws Exception {
        reactor.core.publisher.Sinks.Many<String> sink = reactor.core.publisher.Sinks.many().unicast().onBackpressureBuffer();
        new Thread(() -> {
            try (Connection conn = openConnection();
                 Statement statement = conn.createStatement();
                 ResultSet rs = statement.executeQuery(sql)) {
                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                while (rs.next()) {
                    for (int i = 1; i <= cols; i++) {
                        String colName = md.getColumnLabel(i);
                        sink.tryEmitNext(colName + " => " + String.valueOf(rs.getObject(i)));
                    }
                    sink.tryEmitNext("\n");
                }
                sink.tryEmitComplete();
            } catch (Exception e) {
                sink.tryEmitError(e);
            }
        }).start();
        return sink.asFlux();
    }

    @Tool(description = "输入SQL进行查询，返回JSON数组字符串。对SELECT自动追加 LIMIT 100（如无）以避免过大结果集。")
    public String executeSql(String sql) throws Exception {
        if (sql == null || sql.isBlank()) return "[]";
        String q = sql.trim();
        if (q.toLowerCase(Locale.ROOT).startsWith("select") && !containsLimit(q)) {
            q = q + " LIMIT 100";
        }
        try (Connection conn = openConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(q)) {
            return resultSetToJson(rs);
        }
    }

    @Tool(description = "自然语言查询(中文/English)。可直接输入SQL或类似：‘有哪些表/显示表’、‘查询 user 前10条’、‘查询 user 的 id,name 前5条’、‘user 表有多少条’。返回JSON。")
    public String nlQuery(String question) throws Exception {
        if (question == null || question.isBlank()) return "[]";
        String ql = question.trim();
        String lower = ql.toLowerCase(Locale.ROOT);

        // 直接是 SQL 的情况
        if (looksLikeSql(lower)) {
            return executeSql(ql);
        }

        // 列出所有表
        if (containsAny(lower, "有哪些表", "所有表", "显示表", "列出表", "show tables", "list tables", "tables?", "tables")) {
            return listTablesAndDesc();
        }

        // 简易解析: 表名、列、limit、count
        String table = detectTableNameFromText(ql);
        Integer limit = detectLimit(ql);
        boolean isCount = containsAny(lower, "多少", "多少条", "数量", "count");
        List<String> cols = detectColumns(ql);

        if (table == null || table.isBlank()) {
            // 无法识别表名时返回表清单，帮助上游选择
            return listTablesAndDesc();
        }

        String sql;
        if (isCount) {
            sql = "SELECT COUNT(*) AS count FROM " + quoteIdent(table);
        } else if (!cols.isEmpty()) {
            sql = "SELECT " + String.join(", ", safeColumns(cols)) + " FROM " + quoteIdent(table) +
                    (limit != null ? " LIMIT " + limit : " LIMIT 100");
        } else {
            sql = "SELECT * FROM " + quoteIdent(table) + (limit != null ? " LIMIT " + limit : " LIMIT 100");
        }
        return executeSql(sql);
    }

    @Tool(description = "获取数据库元数据（表和表结构），返回JSON。")
    public String getSchema() throws Exception {
        return listTablesAndDesc();
    }

    // ---------- Internal helpers ----------

    private boolean containsLimit(String sql) {
        return Pattern.compile("\\blimit\\s+\\d+", Pattern.CASE_INSENSITIVE).matcher(sql).find();
    }

    private boolean looksLikeSql(String s) {
        return Pattern.compile("\\b(select|insert|update|delete|create|drop|alter|show|describe|descript)\\b",
                Pattern.CASE_INSENSITIVE).matcher(s).find();
    }

    private boolean containsAny(String s, String... keys) {
        String l = s.toLowerCase(Locale.ROOT);
        for (String k : keys) if (l.contains(k.toLowerCase(Locale.ROOT))) return true;
        return false;
    }

    private String resultSetToJson(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        sb.append("[");
        int r = 0;
        while (rs.next()) {
            if (r++ > 0) sb.append(",");
            sb.append("{");
            for (int i = 1; i <= cols; i++) {
                if (i > 1) sb.append(",");
                String name = md.getColumnLabel(i);
                Object v = rs.getObject(i);
                sb.append("\"").append(escape(name)).append("\":").append(toJsonValue(v));
            }
            sb.append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    private String toJsonValue(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return String.valueOf(v);
        return "\"" + escape(String.valueOf(v)) + "\"";
    }

    private String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    private String quoteIdent(String id) {
        // 简单的标识符清理（不加引号，交由后端处理，如需可扩展）
        return id.replace("`", "");
    }

    private Integer detectLimit(String ql) {
        Matcher m1 = Pattern.compile("(前|top)\\s*(\\d+)\\s*(条|行)?").matcher(ql);
        if (m1.find()) return Integer.parseInt(m1.group(2));
        Matcher m2 = Pattern.compile("limit\\s*(\\d+)", Pattern.CASE_INSENSITIVE).matcher(ql);
        if (m2.find()) return Integer.parseInt(m2.group(1));
        return null;
    }

    private String detectTableNameFromText(String text) {
        // 取“从X/在X/查询X/表X/ X表/ about X”中的 X 为候选；简化处理，仅提取字母数字下划线片段
        Matcher m = Pattern.compile("(?:从|在|查询|表|about|关于)?\\s*([a-zA-Z0-9_]+)\\s*(?:表)?").matcher(text);
        if (m.find()) return m.group(1);
        return null;
    }

    private List<String> detectColumns(String ql) {
        // 简单规则：匹配 “的 a,b,c” 或 “只要 a,b,c” 或 "columns a,b"
        Matcher m = Pattern.compile("(的|只要|仅|只需|columns?)\\s*([a-zA-Z0-9_ ,，]+)").matcher(ql);
        if (!m.find()) return Collections.emptyList();
        String raw = m.group(2).replace("，", ",");
        String[] parts = raw.split(",");
        List<String> cols = new ArrayList<>();
        for (String p : parts) {
            String c = p.trim();
            if (!c.isEmpty()) cols.add(c);
        }
        return cols;
    }

    private List<String> safeColumns(List<String> cols) {
        List<String> out = new ArrayList<>(cols.size());
        for (String c : cols) out.add(quoteIdent(c));
        return out;
    }

    private String listTablesAndDesc() throws Exception {
        try (Connection conn = openConnection();
             Statement st = conn.createStatement()) {
            // SHOW TABLES
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = st.executeQuery("SHOW TABLES")) {
                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                while (rs.next()) {
                    // 使用第1列作为表名（避免依赖列名）
                    String t = String.valueOf(rs.getObject(1));
                    if (t != null && !t.isBlank()) tables.add(t);
                }
            }
            StringBuilder sb = new StringBuilder();
            sb.append("{\"tables\":[");
            for (int i = 0; i < tables.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("\"").append(escape(tables.get(i))).append("\"");
            }
            sb.append("],\"columns\":{");
            for (int i = 0; i < tables.size(); i++) {
                String t = tables.get(i);
                if (i > 0) sb.append(",");
                sb.append("\"").append(escape(t)).append("\":");
                try (ResultSet drs = st.executeQuery("SHOW TABLE DESCRIPT " + t)) {
                    sb.append(resultSetToJson(drs));
                }
            }
            sb.append("}}");
            return sb.toString();
        }
    }
}
package com.dafei1288.jimsql.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Base64;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal PreparedStatement implementation with client-side parameter substitution.
 */
public class JqPreparedStatement implements PreparedStatement {
  private final JqConnection conn;
  private final Statement delegate;
  private final String templateSql; // SQL with '?' placeholders

  // 1-based index parameter map
  private final Map<Integer, Param> params = new LinkedHashMap<>();

  private static final class Param {
    final int sqlType; // Types.NULL for explicit NULL
    final Object value; // raw value
    Param(int sqlType, Object value) { this.sqlType = sqlType; this.value = value; }
  }

  public JqPreparedStatement(JqConnection conn, String sql, Statement delegate) {
    this.conn = conn;
    this.templateSql = sql;
    this.delegate = delegate;
  }

  private static String escapeSqlString(String s) {
    if (s == null) return null;
    return s.replace("'", "''");
  }

  private static boolean isNumericType(int t) {
    return t == Types.INTEGER || t == Types.SMALLINT || t == Types.TINYINT ||
           t == Types.BIGINT || t == Types.DECIMAL || t == Types.NUMERIC ||
           t == Types.FLOAT || t == Types.DOUBLE || t == Types.REAL;
  }

  private static boolean isBooleanType(int t) { return t == Types.BOOLEAN || t == Types.BIT; }

  private String materializeSql() throws SQLException {
    StringBuilder out = new StringBuilder();
    int n = templateSql.length();
    boolean inStr = false; char quote = 0;
    int argIndex = 1;
    for (int i = 0; i < n; i++) {
      char c = templateSql.charAt(i);
      if (inStr) {
        out.append(c);
        if (c == quote) inStr = false;
        continue;
      }
      if (c == '\'' || c == '"') { inStr = true; quote = c; out.append(c); continue; }
      if (c == '?') {
        Param p = params.get(argIndex++);
        if (p == null) throw new SQLException("Parameter " + (argIndex - 1) + " is not set");
        if (p.sqlType == Types.NULL || p.value == null) {
          out.append("NULL");
        } else if (isNumericType(p.sqlType)) {
          out.append(String.valueOf(p.value));
        } else if (isBooleanType(p.sqlType)) {
          out.append(Boolean.TRUE.equals(p.value) ? "true" : "false");
        } else {
          out.append("'").append(escapeSqlString(String.valueOf(p.value))).append("'");
        }
        continue;
      }
      out.append(c);
    }
    return out.toString();
  }

  @Override public ResultSet executeQuery() throws SQLException { return delegate.executeQuery(materializeSql()); }
  @Override public int executeUpdate() throws SQLException { return delegate.executeUpdate(materializeSql()); }
  @Override public boolean execute() throws SQLException { return delegate.execute(materializeSql()); }

  // Parameter setters
  @Override public void setNull(int parameterIndex, int sqlType) { params.put(parameterIndex, new Param(Types.NULL, null)); }
  @Override public void setBoolean(int parameterIndex, boolean x) { params.put(parameterIndex, new Param(Types.BOOLEAN, x)); }
  @Override public void setByte(int parameterIndex, byte x) { params.put(parameterIndex, new Param(Types.TINYINT, x)); }
  @Override public void setShort(int parameterIndex, short x) { params.put(parameterIndex, new Param(Types.SMALLINT, x)); }
  @Override public void setInt(int parameterIndex, int x) { params.put(parameterIndex, new Param(Types.INTEGER, x)); }
  @Override public void setLong(int parameterIndex, long x) { params.put(parameterIndex, new Param(Types.BIGINT, x)); }
  @Override public void setFloat(int parameterIndex, float x) { params.put(parameterIndex, new Param(Types.FLOAT, x)); }
  @Override public void setDouble(int parameterIndex, double x) { params.put(parameterIndex, new Param(Types.DOUBLE, x)); }
  @Override public void setBigDecimal(int parameterIndex, BigDecimal x) { params.put(parameterIndex, new Param(Types.DECIMAL, x)); }
  @Override public void setString(int parameterIndex, String x) { params.put(parameterIndex, new Param(Types.VARCHAR, x)); }
  @Override public void clearParameters() { params.clear(); }

  @Override public void setBytes(int parameterIndex, byte[] x) { params.put(parameterIndex, new Param(Types.VARBINARY, Base64.getEncoder().encodeToString(x))); }
  @Override public void setDate(int parameterIndex, java.sql.Date x) { params.put(parameterIndex, new Param(Types.DATE, x == null ? null : x.toString())); }
  @Override public void setTime(int parameterIndex, java.sql.Time x) { params.put(parameterIndex, new Param(Types.TIME, x == null ? null : x.toString())); }
  @Override public void setTimestamp(int parameterIndex, java.sql.Timestamp x) { params.put(parameterIndex, new Param(Types.TIMESTAMP, x == null ? null : x.toString())); }
  @Override public void setObject(int parameterIndex, Object x) {
    if (x == null) { setNull(parameterIndex, Types.NULL); return; }
    if (x instanceof String) setString(parameterIndex, (String)x);
    else if (x instanceof Integer) setInt(parameterIndex, (Integer)x);
    else if (x instanceof Long) setLong(parameterIndex, (Long)x);
    else if (x instanceof Boolean) setBoolean(parameterIndex, (Boolean)x);
    else if (x instanceof Double) setDouble(parameterIndex, (Double)x);
    else if (x instanceof Float) setFloat(parameterIndex, (Float)x);
    else if (x instanceof BigDecimal) setBigDecimal(parameterIndex, (BigDecimal)x);
    else setString(parameterIndex, String.valueOf(x));
  }
  @Override public void setNull(int parameterIndex, int sqlType, String typeName) { setNull(parameterIndex, sqlType); }

    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException { throw new SQLFeatureNotSupportedException(); }
  @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException { throw new SQLFeatureNotSupportedException(); }
  @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException { throw new SQLFeatureNotSupportedException(); }
  @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException { throw new SQLFeatureNotSupportedException(); }
  @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException { throw new SQLFeatureNotSupportedException(); }
  @Override public boolean execute(String sql, String[] columnNames) throws SQLException { throw new SQLFeatureNotSupportedException(); }// Delegate basics
  @Override public ResultSet executeQuery(String sql) throws SQLException { throw new SQLFeatureNotSupportedException("use executeQuery() without SQL"); }
  @Override public int executeUpdate(String sql) throws SQLException { throw new SQLFeatureNotSupportedException("use executeUpdate() without SQL"); }
  @Override public boolean execute(String sql) throws SQLException { throw new SQLFeatureNotSupportedException("use execute() without SQL"); }
  @Override public void addBatch() throws SQLException { /* not buffered */ }
  @Override public void addBatch(String sql) throws SQLException { throw new SQLFeatureNotSupportedException(); }
  @Override public void clearBatch() throws SQLException { }
  @Override public int[] executeBatch() throws SQLException { throw new SQLFeatureNotSupportedException(); }
  @Override public void close() throws SQLException { delegate.close(); }
  @Override public int getMaxFieldSize() throws SQLException { return delegate.getMaxFieldSize(); }
  @Override public void setMaxFieldSize(int max) throws SQLException { delegate.setMaxFieldSize(max); }
  @Override public int getMaxRows() throws SQLException { return delegate.getMaxRows(); }
  @Override public void setMaxRows(int max) throws SQLException { delegate.setMaxRows(max); }
  @Override public void setEscapeProcessing(boolean enable) throws SQLException { delegate.setEscapeProcessing(enable); }
  @Override public int getQueryTimeout() throws SQLException { return delegate.getQueryTimeout(); }
  @Override public void setQueryTimeout(int seconds) throws SQLException { delegate.setQueryTimeout(seconds); }
  @Override public void cancel() throws SQLException { delegate.cancel(); }
  @Override public SQLWarning getWarnings() throws SQLException { return delegate.getWarnings(); }
  @Override public void clearWarnings() throws SQLException { delegate.clearWarnings(); }
  @Override public void setCursorName(String name) throws SQLException { delegate.setCursorName(name); }
  @Override public Connection getConnection() throws SQLException { return conn; }
  @Override public boolean getMoreResults() throws SQLException { return delegate.getMoreResults(); }
  @Override public ResultSet getGeneratedKeys() throws SQLException { return delegate.getGeneratedKeys(); }
  @Override public int getResultSetHoldability() throws SQLException { return delegate.getResultSetHoldability(); }
  @Override public boolean isClosed() throws SQLException { return delegate.isClosed(); }
  @Override public void setPoolable(boolean poolable) throws SQLException { delegate.setPoolable(poolable); }
  @Override public boolean isPoolable() throws SQLException { return delegate.isPoolable(); }
  @Override public void closeOnCompletion() throws SQLException { delegate.closeOnCompletion(); }
  @Override public boolean isCloseOnCompletion() throws SQLException { return delegate.isCloseOnCompletion(); }
  @Override public ResultSet getResultSet() throws SQLException { return delegate.getResultSet(); }
  @Override public int getUpdateCount() throws SQLException { return delegate.getUpdateCount(); }
  @Override public boolean getMoreResults(int current) throws SQLException { return delegate.getMoreResults(current); }
  @Override public int getFetchDirection() throws SQLException { return delegate.getFetchDirection(); }
  @Override public void setFetchDirection(int direction) throws SQLException { delegate.setFetchDirection(direction); }
  @Override public int getFetchSize() throws SQLException { return delegate.getFetchSize(); }
  @Override public void setFetchSize(int rows) throws SQLException { delegate.setFetchSize(rows); }
  @Override public int getResultSetConcurrency() throws SQLException { return delegate.getResultSetConcurrency(); }
  @Override public int getResultSetType() throws SQLException { return delegate.getResultSetType(); }

    @Override public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length) { throw new UnsupportedOperationException(); }
// Unsupported/unneeded JDBC setters
  @Override public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length) { throw new UnsupportedOperationException(); }
  @Override public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length) { throw new UnsupportedOperationException(); }
  @Override public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) { throw new UnsupportedOperationException(); }
  @Override public void setRef(int parameterIndex, Ref x) { throw new UnsupportedOperationException(); }
  @Override public void setBlob(int parameterIndex, Blob x) { throw new UnsupportedOperationException(); }
  @Override public void setClob(int parameterIndex, Clob x) { throw new UnsupportedOperationException(); }
  @Override public void setArray(int parameterIndex, Array x) { throw new UnsupportedOperationException(); }
  @Override public ResultSetMetaData getMetaData() { return null; }
  @Override public void setDate(int parameterIndex, java.sql.Date x, Calendar cal) { setDate(parameterIndex, x); }
  @Override public void setTime(int parameterIndex, java.sql.Time x, Calendar cal) { setTime(parameterIndex, x); }
  @Override public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal) { setTimestamp(parameterIndex, x); }
  @Override public void setURL(int parameterIndex, java.net.URL x) { setString(parameterIndex, x == null ? null : x.toString()); }
  @Override public ParameterMetaData getParameterMetaData() { return null; }
  @Override public void setRowId(int parameterIndex, RowId x) { throw new UnsupportedOperationException(); }
  @Override public void setNString(int parameterIndex, String value) { setString(parameterIndex, value); }
  @Override public void setNCharacterStream(int parameterIndex, java.io.Reader value, long length) { throw new UnsupportedOperationException(); }
  @Override public void setNClob(int parameterIndex, NClob value) { throw new UnsupportedOperationException(); }
  @Override public void setClob(int parameterIndex, java.io.Reader reader, long length) { throw new UnsupportedOperationException(); }
  @Override public void setBlob(int parameterIndex, java.io.InputStream inputStream, long length) { throw new UnsupportedOperationException(); }
  @Override public void setNClob(int parameterIndex, java.io.Reader reader, long length) { throw new UnsupportedOperationException(); }
  @Override public void setSQLXML(int parameterIndex, SQLXML xmlObject) { throw new UnsupportedOperationException(); }
  @Override public void setObject(int parameterIndex, Object x, int targetSqlType) { setObject(parameterIndex, x); }
  @Override public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) { setObject(parameterIndex, x); }
  @Override public void setAsciiStream(int parameterIndex, java.io.InputStream x, long length) { throw new UnsupportedOperationException(); }
  @Override public void setBinaryStream(int parameterIndex, java.io.InputStream x, long length) { throw new UnsupportedOperationException(); }
  @Override public void setCharacterStream(int parameterIndex, java.io.Reader reader, long length) { throw new UnsupportedOperationException(); }
  @Override public void setAsciiStream(int parameterIndex, java.io.InputStream x) { throw new UnsupportedOperationException(); }
  @Override public void setBinaryStream(int parameterIndex, java.io.InputStream x) { throw new UnsupportedOperationException(); }
  @Override public void setCharacterStream(int parameterIndex, java.io.Reader reader) { throw new UnsupportedOperationException(); }
  @Override public void setNCharacterStream(int parameterIndex, java.io.Reader value) { throw new UnsupportedOperationException(); }
  public void setClob(int parameterIndex, Clob x, long length) { throw new UnsupportedOperationException(); }
  public void setBlob(int parameterIndex, Blob x, long length) { throw new UnsupportedOperationException(); }
  public void setNClob(int parameterIndex, NClob value, long length) { throw new UnsupportedOperationException(); }
  public void setNClob(int parameterIndex, java.io.Reader reader) { throw new UnsupportedOperationException(); }
  public void setBlob(int parameterIndex, java.io.InputStream inputStream) { throw new UnsupportedOperationException(); }
  public void setClob(int parameterIndex, java.io.Reader reader) { throw new UnsupportedOperationException(); }

  public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLFeatureNotSupportedException(); }
  public boolean isWrapperFor(Class<?> iface) { return false; }
}



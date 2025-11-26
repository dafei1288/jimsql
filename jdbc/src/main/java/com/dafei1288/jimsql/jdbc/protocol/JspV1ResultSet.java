package com.dafei1288.jimsql.jdbc.protocol;

import com.dafei1288.jimsql.common.protocol.MessageType;
import com.dafei1288.jimsql.common.protocol.ProtocolFrame;
import com.dafei1288.jimsql.common.protocol.RowBinary;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class JspV1ResultSet implements ResultSet {
  private final JspV1Wire wire;
  private final int colCount;
  private final String[] labels;
  private final int[] types;
  private final String cursorId;
  private final int fetchSize;
  private final Deque<String[]> buffer = new ArrayDeque<>();
  private boolean endReceived = false;
  private String[] currentRow;
  private boolean lastWasNull = false;
  private boolean closed = false;
  private int rowNumber = 0; // 1-based when positioned on a row
  private int fetchDirection = ResultSet.FETCH_FORWARD;

  public JspV1ResultSet(JspV1Wire wire, String headerJson, int fetchSize) throws SQLException {
    this.wire = wire;
    this.labels = parseLabels(headerJson);
    this.types = parseTypes(headerJson, labels.length);
    this.colCount = labels.length;
    this.cursorId = parseCursorId(headerJson);
    this.fetchSize = fetchSize > 0 ? fetchSize : 500;
  }

  private static String[] parseLabels(String json){
    List<String> labs = new ArrayList<>();
    String key = "\"name\":\"";
    int idx = 0;
    while (true) {
      int i = json.indexOf(key, idx);
      if (i < 0) break;
      int s = i + key.length();
      StringBuilder sb = new StringBuilder();
      boolean esc = false;
      for (int p = s; p < json.length(); p++) {
        char ch = json.charAt(p);
        if (esc) { sb.append(ch); esc = false; continue; }
        if (ch == '\\') { esc = true; continue; }
        if (ch == '"') break;
        sb.append(ch);
      }
      labs.add(sb.toString());
      idx = s;
    }
    return labs.toArray(new String[0]);
  }

  private void ensureBuffer() throws SQLException {
    if (!buffer.isEmpty() || endReceived) return;
    try {
      if (cursorId != null) {
        wire.fetch(cursorId, fetchSize);
      }
      ProtocolFrame f = wire.readFrame();
      int t = f.header.type & 0xFF;
      if (t == MessageType.RESULTSET_BATCH.code) {
        byte[] data = f.payload;
        if ((f.header.flags & 0x1) != 0) {
          data = com.dafei1288.jimsql.common.protocol.Compression.unlz4(data);
        }
        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
        List<String[]> rows = RowBinary.readTypedBatchToStrings(bais, colCount, types);
        buffer.addAll(rows);
      } else if (t == MessageType.RESULTSET_END.code) {
        endReceived = true;
      } else if (t == MessageType.ERROR.code) {
        throw new SQLException(new String(f.payload, StandardCharsets.UTF_8));
      } else {
        throw new SQLException("Unexpected frame type: " + t);
      }
    } catch (Exception e) {
      if (e instanceof SQLException) throw (SQLException)e;
      throw new SQLException(e);
    }
  }

  @Override public boolean next() throws SQLException {
    if (buffer.isEmpty() && !endReceived) ensureBuffer();
    if (buffer.isEmpty()) { currentRow = null; return false; }
    currentRow = buffer.pollFirst();
    if (currentRow != null) { rowNumber++; }
    return currentRow != null;
  }

  private static int[] parseTypes(String json, int n) {
    int[] t = new int[n];
    java.util.Arrays.fill(t, java.sql.Types.VARCHAR);
    String key = "\"type\":";
    int idx = 0; int i = 0;
    while (i < n) {
      int p = json.indexOf(key, idx);
      if (p < 0) break;
      int s = p + key.length();
      int e = s;
      while (e < json.length() && (json.charAt(e) == '-' || Character.isDigit(json.charAt(e)))) e++;
      try { t[i] = Integer.parseInt(json.substring(s, e)); } catch (Exception ex) { t[i] = java.sql.Types.VARCHAR; }
      idx = e; i++;
    }
    return t;
  }

  private static String parseCursorId(String json) {
    String pat = "\"cursorId\":\"";
    int i = json.indexOf(pat);
    if (i < 0) return null;
    int s = i + pat.length();
    StringBuilder sb = new StringBuilder();
    boolean esc = false;
    for (int p = s; p < json.length(); p++) {
      char ch = json.charAt(p);
      if (esc) { sb.append(ch); esc = false; continue; }
      if (ch == '\\') { esc = true; continue; }
      if (ch == '"') break;
      sb.append(ch);
    }
    return sb.toString();
  }
  @Override public void close() {
    try {
      if (cursorId != null) {
        wire.closeCursor(cursorId);
      }
    } catch (Exception ignored) {}
    endReceived = true;
    buffer.clear();
    closed = true;
  }
  @Override public boolean wasNull() { return lastWasNull; }

  private String raw(int columnIndex) {
    if (currentRow == null) { lastWasNull = true; return null; }
    int idx = Math.max(1, Math.min(columnIndex, colCount)) - 1;
    String v = currentRow[idx];
    lastWasNull = (v == null);
    return v;
  }

  @Override public String getString(int columnIndex) { return raw(columnIndex); }
  @Override public String getString(String columnLabel) { return getString(findColumn(columnLabel)); }

  @Override public boolean getBoolean(int columnIndex) {
    String v = raw(columnIndex);
    if (v == null) return false;
    String s = v.trim().toLowerCase();
    return ("true".equals(s) || "1".equals(s) || "t".equals(s) || "y".equals(s) || "yes".equals(s));
  }
  @Override public boolean getBoolean(String columnLabel) { return getBoolean(findColumn(columnLabel)); }

  @Override public byte getByte(int columnIndex) {
    String v = raw(columnIndex);
    if (v == null || v.isEmpty()) return 0;
    try { return Byte.parseByte(v.trim()); } catch (Exception e) { return 0; }
  }
  @Override public byte getByte(String columnLabel) { return getByte(findColumn(columnLabel)); }

  @Override public short getShort(int columnIndex) {
    String v = raw(columnIndex);
    if (v == null || v.isEmpty()) return 0;
    try { return Short.parseShort(v.trim()); } catch (Exception e) { return 0; }
  }
  @Override public short getShort(String columnLabel) { return getShort(findColumn(columnLabel)); }

  @Override public int getInt(int columnIndex) {
    String v = raw(columnIndex);
    if (v == null || v.isEmpty()) return 0;
    try { return Integer.parseInt(v.trim()); } catch (Exception e) { return 0; }
  }
  @Override public int getInt(String columnLabel) { return getInt(findColumn(columnLabel)); }

  @Override public long getLong(int columnIndex) {
    String v = raw(columnIndex);
    if (v == null || v.isEmpty()) return 0L;
    try { return Long.parseLong(v.trim()); } catch (Exception e) { return 0L; }
  }
  @Override public long getLong(String columnLabel) { return getLong(findColumn(columnLabel)); }

  @Override public float getFloat(int columnIndex) {
    String v = raw(columnIndex);
    if (v == null || v.isEmpty()) return 0f;
    try { return Float.parseFloat(v.trim()); } catch (Exception e) { return 0f; }
  }
  @Override public float getFloat(String columnLabel) { return getFloat(findColumn(columnLabel)); }

  @Override public double getDouble(int columnIndex) {
    String v = raw(columnIndex);
    if (v == null || v.isEmpty()) return 0d;
    try { return Double.parseDouble(v.trim()); } catch (Exception e) { return 0d; }
  }
  @Override public double getDouble(String columnLabel) { return getDouble(findColumn(columnLabel)); }

  @Override public BigDecimal getBigDecimal(int columnIndex, int scale) {
    String v = raw(columnIndex);
    if (v == null || v.isEmpty()) return null;
    try { return new BigDecimal(v.trim()).setScale(scale, BigDecimal.ROUND_HALF_UP); } catch (Exception e) { return null; }
  }
  @Override public BigDecimal getBigDecimal(int columnIndex) { String v = raw(columnIndex); if (v==null||v.isEmpty()) return null; try { return new BigDecimal(v.trim()); } catch (Exception e) { return null; } }
  @Override public BigDecimal getBigDecimal(String columnLabel, int scale) { return getBigDecimal(findColumn(columnLabel), scale); }
  @Override public BigDecimal getBigDecimal(String columnLabel) { return getBigDecimal(findColumn(columnLabel)); }

  @Override public byte[] getBytes(int columnIndex) {
    String v = raw(columnIndex);
    return v == null ? null : v.getBytes(StandardCharsets.UTF_8);
  }
  @Override public byte[] getBytes(String columnLabel) { return getBytes(findColumn(columnLabel)); }

  private static Date parseDate(String v) {
    try { return Date.valueOf(v.trim()); } catch (Exception ignore) { return null; }
  }
  private static Time parseTime(String v) {
    try { return Time.valueOf(v.trim()); } catch (Exception ignore) { return null; }
  }
  private static Timestamp parseTimestamp(String v) {
    try { return Timestamp.valueOf(v.trim()); } catch (Exception ignore) { return null; }
  }

  @Override public Date getDate(int columnIndex) { String v = raw(columnIndex); return v==null?null:parseDate(v); }
  @Override public Date getDate(String columnLabel) { return getDate(findColumn(columnLabel)); }
  @Override public Time getTime(int columnIndex) { String v = raw(columnIndex); return v==null?null:parseTime(v); }
  @Override public Time getTime(String columnLabel) { return getTime(findColumn(columnLabel)); }
  @Override public Timestamp getTimestamp(int columnIndex) { String v = raw(columnIndex); return v==null?null:parseTimestamp(v); }
  @Override public Timestamp getTimestamp(String columnLabel) { return getTimestamp(findColumn(columnLabel)); }

  @Override public InputStream getAsciiStream(int columnIndex) { String v = raw(columnIndex); return v==null?null:new java.io.ByteArrayInputStream(v.getBytes(java.nio.charset.StandardCharsets.US_ASCII)); }
  @Override public InputStream getUnicodeStream(int columnIndex) { String v = raw(columnIndex); return v==null?null:new java.io.ByteArrayInputStream(v.getBytes(java.nio.charset.StandardCharsets.UTF_16)); }
  @Override public InputStream getBinaryStream(int columnIndex) { String v = raw(columnIndex); return v==null?null:new java.io.ByteArrayInputStream(v.getBytes(StandardCharsets.UTF_8)); }
  @Override public InputStream getAsciiStream(String columnLabel) { return getAsciiStream(findColumn(columnLabel)); }
  @Override public InputStream getUnicodeStream(String columnLabel) { return getUnicodeStream(findColumn(columnLabel)); }
  @Override public InputStream getBinaryStream(String columnLabel) { return getBinaryStream(findColumn(columnLabel)); }
  @Override public SQLWarning getWarnings() { return null; }
  @Override public void clearWarnings() {}
  @Override public String getCursorName() { return cursorId; }
  @Override public ResultSetMetaData getMetaData() { return new Meta(labels, types); }
  @Override public Object getObject(int columnIndex) {
    // Return a best-effort typed object based on declared SQL type
    int t = (columnIndex-1)>=0 && (columnIndex-1)<types.length ? types[columnIndex-1] : java.sql.Types.VARCHAR;
    String v = raw(columnIndex);
    if (v == null) return null;
    try {
      return switch (t) {
        case java.sql.Types.INTEGER -> Integer.parseInt(v.trim());
        case java.sql.Types.BIGINT -> Long.parseLong(v.trim());
        case java.sql.Types.DOUBLE, java.sql.Types.FLOAT, java.sql.Types.REAL -> Double.parseDouble(v.trim());
        case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> getBoolean(columnIndex);
        case java.sql.Types.DATE -> parseDate(v);
        case java.sql.Types.TIME -> parseTime(v);
        case java.sql.Types.TIMESTAMP -> parseTimestamp(v);
        default -> v;
      };
    } catch (Exception ignore) { return v; }
  }
  @Override public Object getObject(String columnLabel) { return getObject(findColumn(columnLabel)); }
  @Override public int findColumn(String columnLabel) { for (int i=0;i<labels.length;i++){ if (labels[i].equalsIgnoreCase(columnLabel)) return i+1; } return 1; }
  @Override public Reader getCharacterStream(int columnIndex) { String v = raw(columnIndex); return v==null?null:new java.io.StringReader(v); }
  @Override public Reader getCharacterStream(String columnLabel) { return getCharacterStream(findColumn(columnLabel)); }
  @Override public boolean isBeforeFirst() { return rowNumber == 0 && (!endReceived || !buffer.isEmpty()); }
  @Override public boolean isAfterLast() { return endReceived && buffer.isEmpty() && currentRow == null; }
  @Override public boolean isFirst() { return rowNumber == 1; }
  @Override public boolean isLast() { return endReceived && buffer.isEmpty() && currentRow != null; }
  @Override public void beforeFirst() {}
  @Override public void afterLast() {}
  @Override public boolean first() { return false; }
  @Override public boolean last() { return false; }
  @Override public int getRow() { return rowNumber; }
  @Override public boolean absolute(int row) { return false; }
  @Override public boolean relative(int rows) { return false; }
  @Override public boolean previous() { return false; }
  @Override public void setFetchDirection(int direction) { if (direction == ResultSet.FETCH_FORWARD) this.fetchDirection = direction; }
  @Override public int getFetchDirection() { return this.fetchDirection; }
  @Override public void setFetchSize(int rows) {}
  @Override public int getFetchSize() { return this.fetchSize; }
  @Override public int getType() { return ResultSet.TYPE_FORWARD_ONLY; }
  @Override public int getConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
  @Override public boolean rowUpdated() { return false; }
  @Override public boolean rowInserted() { return false; }
  @Override public boolean rowDeleted() { return false; }
  @Override public void updateNull(int columnIndex) {}
  @Override public void updateBoolean(int columnIndex, boolean x) {}
  @Override public void updateByte(int columnIndex, byte x) {}
  @Override public void updateShort(int columnIndex, short x) {}
  @Override public void updateInt(int columnIndex, int x) {}
  @Override public void updateLong(int columnIndex, long x) {}
  @Override public void updateFloat(int columnIndex, float x) {}
  @Override public void updateDouble(int columnIndex, double x) {}
  @Override public void updateBigDecimal(int columnIndex, BigDecimal x) {}
  @Override public void updateString(int columnIndex, String x) {}
  @Override public void updateBytes(int columnIndex, byte[] x) {}
  @Override public void updateDate(int columnIndex, Date x) {}
  @Override public void updateTime(int columnIndex, Time x) {}
  @Override public void updateTimestamp(int columnIndex, Timestamp x) {}
  @Override public void updateAsciiStream(int columnIndex, InputStream x, int length) {}
  @Override public void updateBinaryStream(int columnIndex, InputStream x, int length) {}
  @Override public void updateCharacterStream(int columnIndex, Reader x, int length) {}
  @Override public void updateObject(int columnIndex, Object x, int scaleOrLength) {}
  @Override public void updateObject(int columnIndex, Object x) {}
  @Override public void updateNull(String columnLabel) {}
  @Override public void updateBoolean(String columnLabel, boolean x) {}
  @Override public void updateByte(String columnLabel, byte x) {}
  @Override public void updateShort(String columnLabel, short x) {}
  @Override public void updateInt(String columnLabel, int x) {}
  @Override public void updateLong(String columnLabel, long x) {}
  @Override public void updateFloat(String columnLabel, float x) {}
  @Override public void updateDouble(String columnLabel, double x) {}
  @Override public void updateBigDecimal(String columnLabel, BigDecimal x) {}
  @Override public void updateString(String columnLabel, String x) {}
  @Override public void updateBytes(String columnLabel, byte[] x) {}
  @Override public void updateDate(String columnLabel, Date x) {}
  @Override public void updateTime(String columnLabel, Time x) {}
  @Override public void updateTimestamp(String columnLabel, Timestamp x) {}
  @Override public void updateAsciiStream(String columnLabel, InputStream x, int length) {}
  @Override public void updateBinaryStream(String columnLabel, InputStream x, int length) {}
  @Override public void updateCharacterStream(String columnLabel, Reader reader, int length) {}
  @Override public void updateObject(String columnLabel, Object x, int scaleOrLength) {}
  @Override public void updateObject(String columnLabel, Object x) {}
  @Override public void insertRow() {}
  @Override public void updateRow() {}
  @Override public void deleteRow() {}
  @Override public void refreshRow() {}
  @Override public void cancelRowUpdates() {}
  @Override public void moveToInsertRow() {}
  @Override public void moveToCurrentRow() {}
  @Override public Statement getStatement() { return null; }
  @Override public Object getObject(int columnIndex, java.util.Map<String,Class<?>> map) { return null; }
  @Override public Ref getRef(int columnIndex) { return null; }
  @Override public Blob getBlob(int columnIndex) { return null; }
  @Override public Clob getClob(int columnIndex) { return null; }
  @Override public Array getArray(int columnIndex) { return null; }
  @Override public Object getObject(String columnLabel, java.util.Map<String,Class<?>> map) { return null; }
  @Override public Ref getRef(String columnLabel) { return null; }
  @Override public Blob getBlob(String columnLabel) { return null; }
  @Override public Clob getClob(String columnLabel) { return null; }
  @Override public Array getArray(String columnLabel) { return null; }
  @Override public Date getDate(int columnIndex, java.util.Calendar cal) { return null; }
  @Override public Date getDate(String columnLabel, java.util.Calendar cal) { return null; }
  @Override public Time getTime(int columnIndex, java.util.Calendar cal) { return null; }
  @Override public Time getTime(String columnLabel, java.util.Calendar cal) { return null; }
  @Override public Timestamp getTimestamp(int columnIndex, java.util.Calendar cal) { return null; }
  @Override public Timestamp getTimestamp(String columnLabel, java.util.Calendar cal) { return null; }
  @Override public URL getURL(int columnIndex) { return null; }
  @Override public URL getURL(String columnLabel) { return null; }
  @Override public void updateRef(int columnIndex, Ref x) {}
  @Override public void updateRef(String columnLabel, Ref x) {}
  @Override public void updateBlob(int columnIndex, Blob x) {}
  @Override public void updateBlob(String columnLabel, Blob x) {}
  @Override public void updateClob(int columnIndex, Clob x) {}
  @Override public void updateClob(String columnLabel, Clob x) {}
  @Override public void updateArray(int columnIndex, Array x) {}
  @Override public void updateArray(String columnLabel, Array x) {}
  @Override public RowId getRowId(int columnIndex) { return null; }
  @Override public RowId getRowId(String columnLabel) { return null; }
  @Override public void updateRowId(int columnIndex, RowId x) {}
  @Override public void updateRowId(String columnLabel, RowId x) {}
  @Override public int getHoldability() { return ResultSet.HOLD_CURSORS_OVER_COMMIT; }
  @Override public boolean isClosed() { return closed; }
  @Override public void updateNString(int columnIndex, String nString) {}
  @Override public void updateNString(String columnLabel, String nString) {}
  @Override public void updateNClob(int columnIndex, NClob nClob) {}
  @Override public void updateNClob(String columnLabel, NClob nClob) {}
  @Override public NClob getNClob(int columnIndex) { return null; }
  @Override public NClob getNClob(String columnLabel) { return null; }
  @Override public SQLXML getSQLXML(int columnIndex) { return null; }
  @Override public SQLXML getSQLXML(String columnLabel) { return null; }
  @Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) {}
  @Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) {}
  @Override public String getNString(int columnIndex) { return null; }
  @Override public String getNString(String columnLabel) { return null; }
  @Override public Reader getNCharacterStream(int columnIndex) { return null; }
  @Override public Reader getNCharacterStream(String columnLabel) { return null; }
  @Override public void updateNCharacterStream(int columnIndex, Reader x, long length) {}
  @Override public void updateNCharacterStream(String columnLabel, Reader reader, long length) {}
  @Override public void updateAsciiStream(int columnIndex, InputStream x, long length) {}
  @Override public void updateBinaryStream(int columnIndex, InputStream x, long length) {}
  @Override public void updateCharacterStream(int columnIndex, Reader x, long length) {}
  @Override public void updateAsciiStream(String columnLabel, InputStream x, long length) {}
  @Override public void updateBinaryStream(String columnLabel, InputStream x, long length) {}
  @Override public void updateCharacterStream(String columnLabel, Reader reader, long length) {}

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object v = getObject(columnIndex);
        try { return type.cast(v); } catch (Exception e) { return null; }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        Object v = getObject(columnLabel);
        try { return type.cast(v); } catch (Exception e) { return null; }
    }

    @Override public <T> T unwrap(Class<T> iface) { return null; }
  @Override public boolean isWrapperFor(Class<?> iface) { return false; }

  static class Meta implements ResultSetMetaData {
    private final String[] labels;
    private final int[] types;
    Meta(String[] labels, int[] types){ this.labels = labels; this.types = types; }
    @Override public int getColumnCount() { return labels.length; }
    @Override public String getColumnLabel(int column) { int idx = Math.max(1, Math.min(column, labels.length)) - 1; return labels[idx]; }
    @Override public String getColumnName(int column) { return getColumnLabel(column); }
    @Override public int getColumnType(int column) { int idx = Math.max(1, Math.min(column, types.length)) - 1; return types[idx]; }
    @Override public String getColumnTypeName(int column) {
      return switch (getColumnType(column)) {
        case java.sql.Types.INTEGER -> "INTEGER";
        case java.sql.Types.BIGINT -> "BIGINT";
        case java.sql.Types.DOUBLE -> "DOUBLE";
        case java.sql.Types.FLOAT -> "FLOAT";
        case java.sql.Types.REAL -> "REAL";
        case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> "BOOLEAN";
        case java.sql.Types.DATE -> "DATE";
        case java.sql.Types.TIME -> "TIME";
        case java.sql.Types.TIMESTAMP -> "TIMESTAMP";
        default -> "VARCHAR";
      };
    }
    @Override public <T> T unwrap(Class<T> iface) { return null; }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    @Override public boolean isAutoIncrement(int column) { return false; }
    @Override public boolean isCaseSensitive(int column) { return true; }
    @Override public boolean isSearchable(int column) { return true; }
    @Override public boolean isCurrency(int column) { return false; }
    @Override public int isNullable(int column) { return ResultSetMetaData.columnNullable; }
    @Override public boolean isSigned(int column) { return false; }
    @Override public int getColumnDisplaySize(int column) { return 255; }
    @Override public String getSchemaName(int column) { return ""; }
    @Override public int getPrecision(int column) { return 0; }
    @Override public int getScale(int column) { return 0; }
    @Override public String getTableName(int column) { return ""; }
    @Override public String getCatalogName(int column) { return ""; }
    @Override public boolean isReadOnly(int column) { return false; }
    @Override public boolean isWritable(int column) { return true; }
    @Override public boolean isDefinitelyWritable(int column) { return true; }
    @Override public String getColumnClassName(int column) {
      return switch (getColumnType(column)) {
        case java.sql.Types.INTEGER -> Integer.class.getName();
        case java.sql.Types.BIGINT -> Long.class.getName();
        case java.sql.Types.DOUBLE, java.sql.Types.FLOAT, java.sql.Types.REAL -> Double.class.getName();
        case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> Boolean.class.getName();
        case java.sql.Types.DATE -> java.sql.Date.class.getName();
        case java.sql.Types.TIME -> java.sql.Time.class.getName();
        case java.sql.Types.TIMESTAMP -> java.sql.Timestamp.class.getName();
        default -> String.class.getName();
      };
    }
  }
}

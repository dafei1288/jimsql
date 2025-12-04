package com.dafei1288.jimsql.jdbc;

import com.dafei1288.jimsql.common.JimSQueryStatus;
import com.dafei1288.jimsql.common.JqQueryReq;
import com.dafei1288.jimsql.common.JqResultSetMetaData;
import io.netty.handler.codec.serialization.ObjectDecoderInputStream;
import io.netty.handler.codec.serialization.ObjectEncoderOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class JqStatement implements Statement {

//  private String datapath;
//  JqStatement(String datapath){
//    this.datapath = datapath;
//  }

  private JqConnection jqConnection;
//  private PrintWriter out;
  private ObjectEncoderOutputStream out;
  private InputStream in;
  private JqResultSet lastResultSet;
  private int lastUpdateCount = -1;

  public JqStatement(JqConnection jqConnection) throws SQLException{
    this.jqConnection = jqConnection;
    try {
      //out = new PrintWriter(jqConnection.getClientSocket().getOutputStream(), true);
      out = new ObjectEncoderOutputStream(jqConnection.getClientSocket().getOutputStream());
      in = jqConnection.getClientSocket().getInputStream();
    }catch (Exception e){
      throw new SQLException();
    }
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    JqQueryReq jqQueryReq = new JqQueryReq();
    jqQueryReq.setSql(sql);
    jqQueryReq.setDb(this.jqConnection.getInfo().getProperty("db"));
//    out.println(jqQueryReq);
    try {
      out.writeObject(jqQueryReq);
    } catch (IOException e) {
      throw new SQLWarning(e);
    }
    ObjectDecoderInputStream decoderInputStream = new ObjectDecoderInputStream(in);
    JqResultSet jqResultSet = null;

    try {
      Object obj = decoderInputStream.readObject();
      if(obj instanceof JimSQueryStatus && JimSQueryStatus.BEGIN.equals((JimSQueryStatus) obj)){
        //???metadata
        obj =  decoderInputStream.readObject();

        jqResultSet = new JqResultSet(decoderInputStream);

        if(obj instanceof JqResultSetMetaData){
          jqResultSet.setJqResultSetMetaData((JqResultSetMetaData) obj);
        }
      }

    }catch (Exception e){
      e.printStackTrace();
      throw new SQLException("can?? start query ");
    }
    return jqResultSet;
  }

  //  @Override
//  public ResultSet executeQuery(String sql) throws SQLException {
//
//    Pattern pattern = Pattern.compile("^select (\\*|[a-z0-9,]*) from ([a-z]+)");
//    Matcher matcher = pattern.matcher(sql);
//    String tableName = "";
//    List<String> cols = null;
//    if(matcher.find()){
//      String cs = matcher.group(1);
//      if(cs.equals("*")){
//      }else{
//        String[] cns =cs.split(",");
//        cols = Arrays.stream(cns).toList();
//      }
//      tableName = matcher.group(2);
//    }
//    JqResultSet jqResultSet = null;//new JqResultSet(datapath,cols,tableName);
//
//    return jqResultSet;
//  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    boolean hasRs = execute(sql);
    if (hasRs) throw new SQLException("Query returned a result set");
    return (lastUpdateCount < 0) ? 0 : lastUpdateCount;
  }
    return 0;
  }

  @Override
  public void close() throws SQLException {
    this.jqConnection = null;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {

  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {

  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {

  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {

  }

  @Override
  public void cancel() throws SQLException {

  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public void setCursorName(String name) throws SQLException {

  }

  @Override
    public boolean execute(String sql) throws SQLException {
    String t = sql.trim().toLowerCase(java.util.Locale.ROOT);
    if (t.startsWith("select") || t.startsWith("show") || t.startsWith("describe") || t.startsWith("explain")) {
      this.lastResultSet = (JqResultSet) this.executeQuery(sql);
      this.lastUpdateCount = -1;
      return true;
    } else {
      // send as generic request and wait for FINISH/OK
      JqQueryReq jqQueryReq = new JqQueryReq();
      jqQueryReq.setSql(sql);
      jqQueryReq.setDb(this.jqConnection.getInfo().getProperty("db"));
      try { out.writeObject(jqQueryReq); } catch (IOException e) { throw new SQLWarning(e); }
      ObjectDecoderInputStream decoder = new ObjectDecoderInputStream(in);
      try {
        Object obj;
        while ((obj = decoder.readObject()) != null) {
          if (obj instanceof Integer) { this.lastUpdateCount = ((Integer)obj).intValue(); continue; }
          if (obj instanceof JimSQueryStatus) {
            JimSQueryStatus s = (JimSQueryStatus) obj;
            if (s.equals(JimSQueryStatus.OK)) { if (this.lastUpdateCount < 0) this.lastUpdateCount = 0; }
            if (s.equals(JimSQueryStatus.FINISH)) break;
          }
        }
      } catch (Exception e) { throw new SQLException(e); }
      this.lastResultSet = null;
      return false;
    }
  }@Override
  public ResultSet getResultSet() throws SQLException { return lastResultSet; }

  @Override
  public int getUpdateCount() throws SQLException { return lastUpdateCount; }

  @Override
  public boolean getMoreResults() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {

  }

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {

  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {

  }

  @Override
  public void clearBatch() throws SQLException {

  }

  @Override
  public int[] executeBatch() throws SQLException {
    return new int[0];
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    return 0;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {

  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {

  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}


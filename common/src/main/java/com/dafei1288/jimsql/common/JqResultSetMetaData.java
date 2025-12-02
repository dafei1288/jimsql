package com.dafei1288.jimsql.common;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;

public class JqResultSetMetaData implements ResultSetMetaData, Serializable {
//  public JqResultSetMetaData(){
//
//  }

  public JqResultSetMetaData(LinkedHashMap<String, JqColumnResultSetMetadata> columnMeta) {
    this._columnMeta = columnMeta;
  }

  private LinkedHashMap<String, JqColumnResultSetMetadata> _columnMeta;

  public LinkedHashMap<String, JqColumnResultSetMetadata> getColumnMeta() {
    return _columnMeta;
  }

  public void setColumnMeta(
      LinkedHashMap<String, JqColumnResultSetMetadata> columnMeta) {
    this._columnMeta = columnMeta;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return this._columnMeta.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return 0;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return this._columnMeta.values().stream().skip(column-1).findFirst().get().getLabelName();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return this._columnMeta.values().stream().skip(column-1).findFirst().get().getLabelName();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return null;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return this._columnMeta.values().stream().skip(column-1).findFirst().get().getTableName();
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return null;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return this._columnMeta.values().stream().skip(column-1).findFirst().get().getColumnType();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return this._columnMeta.values().stream().skip(column-1).findFirst().get().getClazzStr();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return null;
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

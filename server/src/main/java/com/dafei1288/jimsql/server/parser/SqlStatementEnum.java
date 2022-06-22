package com.dafei1288.jimsql.server.parser;

public enum SqlStatementEnum {
//  ddl
  CREATE_DATABASE,
  DROP_DATABASE,
  USE_DATABASE,
  CREATE_TABLE,
  DROP_TABLE,


//  dml
  INSERT_TABLE,
  DELETE_TABLE,
  UPDATE_TABLE,


//  dql
  SELECT_TABLE,
  EXPLAIN_SELECT_TABLE,

//  dcl
  SHOW_PROCESSLIST,
  SHOW_DATABASES,
  SHOW_TABLES,
  SHOW_TABLEDESC

}

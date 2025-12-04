package com.dafei1288.jimsql.server.instance;

import com.dafei1288.jimsql.common.JqColumnResultSetMetadata;
import com.dafei1288.jimsql.common.Utils;
import com.dafei1288.jimsql.common.meta.JqColumn;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.Jimserver;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ServerMetadata {
  private static ServerMetadata serverMetadata;

  public static ServerMetadata getInstance(){
    if(serverMetadata != null) return  serverMetadata;

    synchronized (ServerMetadata.class){
      if(serverMetadata == null) {
        serverMetadata = new ServerMetadata();
      }
    }

    return serverMetadata;
  }

  private String datadir;

  private LinkedHashMap<String,JqDatabase> jqDatabaseLinkedHashMap;
  private ServerMetadata(){
    String dd = Jimserver.getDataDir();
if (dd != null && dd.length() >= 2) {
  char f = dd.charAt(0), l = dd.charAt(dd.length()-1);
  if ((f=='\"' && l=='\"') || (f=='\'' && l=='\'')) {
    dd = dd.substring(1, dd.length()-1);
  }
}
datadir = dd;
    try {
       jqDatabaseLinkedHashMap = Files.list(new File(datadir).toPath())
                          .filter(it->it.toFile().isDirectory())
                          .map(it->{
                            JqDatabase jqDatabase = new JqDatabase();
                            jqDatabase.setBasePath(it.toFile());
                            jqDatabase.setDatabaseName(it.toFile().getName());
                            File[] csvs = it.toFile().listFiles(new FileFilter() {
                              @Override
                              public boolean accept(File pathname) {
                                return pathname.isFile() && pathname.getName().endsWith(Utils.DB_FILENAME_SUFFIX);
                              }
                            });

                            LinkedHashMap<String,JqTable> jqTableList = Arrays.stream(csvs).map(csvFile->{
                                JqTable jqTable = new JqTable();
                                String tableName = csvFile.getName();
                                try {
                                  tableName = csvFile.getName().split("\\.")[0];
                                }catch (Exception e){

                                }
                                jqTable.setBasepath(csvFile);
                                jqTable.setTableName(tableName);
                                jqTable.setJqDatabase(jqDatabase);

                              try {
                                java.io.BufferedReader br = Files.newBufferedReader(csvFile.toPath());
                                String firtLine = br.readLine();
                                String[] columns = firtLine.split(Utils.COLUMN_SPILTOR);
                                String secondLine = br.readLine();
                                if (secondLine == null) secondLine = "";
                                String[] sample = secondLine.split(Utils.COLUMN_SPILTOR, -1);
                                LinkedHashMap<String,JqColumn> jqColumnList = Arrays.stream(columns).map(column->{
                                    JqColumn jqColumn = new JqColumn();
                                    jqColumn.setColumnName(column);
                                    jqColumn.setTable(jqTable);
                                    int idx = Arrays.asList(columns).indexOf(column);
                                    String val = (idx >=0 && idx < sample.length) ? sample[idx] : null;
                                    if (val != null && val.matches("-?\\d+")) {
                                      try { long l = Long.parseLong(val); if (l>=Integer.MIN_VALUE && l<=Integer.MAX_VALUE) { jqColumn.setColumnType(Types.INTEGER); jqColumn.setColumnClazzType(Integer.class);} else { jqColumn.setColumnType(Types.BIGINT); jqColumn.setColumnClazzType(Long.class);} }
                                      catch (Exception e){ jqColumn.setColumnType(Types.INTEGER); jqColumn.setColumnClazzType(Integer.class);} }
                                    else if (val != null && val.matches("-?((\\d+\\.\\d+)|(\\d+))")) { jqColumn.setColumnType(Types.DOUBLE); jqColumn.setColumnClazzType(Double.class); }
                                    else { jqColumn.setColumnType(Types.VARCHAR); jqColumn.setColumnClazzType(String.class); }
                                    jqColumn.setSize(50);
                                    return jqColumn;
                                }).collect(Collectors.toMap(JqColumn::getColumnName, Function.identity(), (oldValue, newValue) -> oldValue, LinkedHashMap::new));
                                jqTable.setJqTableLinkedHashMap(jqColumnList);
                              } catch (IOException e) {
                                throw new RuntimeException(e);
                              }


                                return jqTable;
                            }).collect(Collectors.toMap(JqTable::getTableName, Function.identity(), (oldValue, newValue) -> oldValue, LinkedHashMap::new));

                            jqDatabase.setJqTableListMap(jqTableList);
                            return  jqDatabase;
                          })
                          .collect(Collectors.toMap(JqDatabase::getDatabaseName, Function.identity(), (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public LinkedHashMap<String,JqDatabase> getJqDatabaseLinkedHashMap() {
    return jqDatabaseLinkedHashMap;
  }

  public void setJqDatabaseLinkedHashMap(LinkedHashMap<String,JqDatabase> jqDatabaseLinkedHashMap) {
    this.jqDatabaseLinkedHashMap = jqDatabaseLinkedHashMap;
  }

  public JqDatabase fetchDatabaseByName(String dbName){
    return this.jqDatabaseLinkedHashMap.get(dbName);
  }

  public JqTable fetchTableByName(String dbName,String tableName){
    return fetchDatabaseByName(dbName).getJqTableListMap().get(tableName);
  }

  public List<JqColumn> fetchColumnsByName(String currentDatabaseName, String currentTableName, List<String> colNames) {
    List<JqColumn> res = new ArrayList<>();
    JqTable jqTable = fetchTableByName(currentDatabaseName,currentTableName);
    for(String colName : colNames){
      JqColumn jqColumn = jqTable.getJqTableLinkedHashMap().get(colName);
      res.add(jqColumn);
    }
    return res;
  }
}

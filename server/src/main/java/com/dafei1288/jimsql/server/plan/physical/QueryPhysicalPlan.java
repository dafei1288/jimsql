package com.dafei1288.jimsql.server.plan.physical;

import com.dafei1288.jimsql.common.JqColumnResultSetMetadata;
import com.dafei1288.jimsql.common.RowData;
import com.dafei1288.jimsql.common.Utils;
import com.dafei1288.jimsql.common.meta.JqDatabase;
import com.dafei1288.jimsql.common.meta.JqTable;
import com.dafei1288.jimsql.server.instance.ServerMetadata;
import com.dafei1288.jimsql.server.plan.logical.LogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.OptimizeQueryLogicalPlan;
import com.google.common.io.Files;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class QueryPhysicalPlan implements PhysicalPlan{
  private LogicalPlan logicalPlan;



  @Override
  public void setLogicalPlan(LogicalPlan logicalPlan) {
    this.logicalPlan = logicalPlan;
  }

  @Override
  public LogicalPlan getLogicalPlan() {
    return logicalPlan;
  }

  @Override
  public void proxyWrite(ChannelHandlerContext ctx) throws IOException {
    OptimizeQueryLogicalPlan optimizeQueryLogicalPlan = (OptimizeQueryLogicalPlan)logicalPlan;
    String currentDatabase = optimizeQueryLogicalPlan.getCurrentDatabase().getDatabaseName();
    String currentTable = optimizeQueryLogicalPlan.getQueryLogicalPlan().getFromTable().getTableName();
    JqDatabase jqDatabase = ServerMetadata.getInstance().fetchDatabaseByName(currentDatabase);
    JqTable jqTable = ServerMetadata.getInstance().fetchTableByName(currentDatabase,currentTable);
    List<String> datas = Files.readLines(jqTable.getBasepath(), Charset.defaultCharset());

    for(int i = 1; i < datas.size(); i++){
      RowData rowData = new RowData();

      String data = datas.get(i);
      String[] rowdataStr = data.split(Utils.COLUMN_SPILTOR);
      int j = 0;
      LinkedHashMap<String,String> _table = new LinkedHashMap<>();
      LinkedHashMap<String,Object> datatrans = new LinkedHashMap<>();
      for(String key : jqTable.getJqTableLinkedHashMap().keySet()){
        String value = rowdataStr[j++];
//        _table.put(key,rowdataStr[j++]);
        JqColumnResultSetMetadata jcrsm = optimizeQueryLogicalPlan.getJqColumnResultSetMetadataList().get(key);
        if(jcrsm!=null){
          datatrans.put(key,value);
        }
      }
      rowData.setNext(true);
      rowData.setDatas(datatrans);


      ctx.writeAndFlush(rowData);
    }


  }
}

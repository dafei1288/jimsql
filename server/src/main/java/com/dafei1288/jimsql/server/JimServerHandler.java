package com.dafei1288.jimsql.server;

import com.dafei1288.jimsql.common.JimSQueryStatus;
import com.dafei1288.jimsql.common.JqColumnMetadata;
import com.dafei1288.jimsql.common.JqResultSetMetaData;
import com.dafei1288.jimsql.common.RowData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;


public class JimServerHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ctx.writeAndFlush(JimSQueryStatus.BEGIN);
    ctx.flush();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    //super.channelRead(ctx,msg);

    String sql = msg.toString();
    System.out.println("sql will run : " + sql +" , on "+ctx.hashCode());
    if(sql.contains("select")){



      LinkedHashMap<String,JqColumnMetadata> jqColumnMetadataList = new LinkedHashMap<>();

      JqColumnMetadata jqColumnMetadata = new JqColumnMetadata();
      jqColumnMetadata.setIndex(1);
      jqColumnMetadata.setLabelName("id");
      jqColumnMetadata.setClazz(String.class);
      jqColumnMetadata.setClazzStr("String");
      jqColumnMetadata.setTableName("user");
      jqColumnMetadata.setColumnType(1);
      jqColumnMetadataList.put(jqColumnMetadata.getLabelName(),jqColumnMetadata);


      jqColumnMetadata = new JqColumnMetadata();
      jqColumnMetadata.setIndex(2);
      jqColumnMetadata.setLabelName("name");
      jqColumnMetadata.setClazz(String.class);
      jqColumnMetadata.setClazzStr("String");
      jqColumnMetadata.setTableName("user");
      jqColumnMetadata.setColumnType(2);
      jqColumnMetadataList.put(jqColumnMetadata.getLabelName(),jqColumnMetadata);

      jqColumnMetadata = new JqColumnMetadata();
      jqColumnMetadata.setIndex(3);
      jqColumnMetadata.setLabelName("age");
      jqColumnMetadata.setClazz(Integer.class);
      jqColumnMetadata.setClazzStr("Integer");
      jqColumnMetadata.setTableName("user");
      jqColumnMetadata.setColumnType(1);
      jqColumnMetadataList.put(jqColumnMetadata.getLabelName(),jqColumnMetadata);

      JqResultSetMetaData jqResultSetMetaData = new JqResultSetMetaData(jqColumnMetadataList);
      jqResultSetMetaData.setColumnMeta(jqColumnMetadataList);

      ctx.writeAndFlush(jqResultSetMetaData);

//      List<RowData> datas = new ArrayList<>();
      for(int i = 0;i<10;i++){
        RowData rowData = new RowData();
        rowData.setNext(true);
        if(i==9){
          rowData.setNext(false);
        }
        LinkedHashMap<String,Object> map = new LinkedHashMap<>();
        map.put("id","id"+i);
        map.put("name","name"+i);
        map.put("age",i);
        rowData.setDatas(map);
        ctx.writeAndFlush(rowData);
      }



    }else{
      ctx.writeAndFlush(JimSQueryStatus.OK);
    }
    ctx.flush();
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.writeAndFlush(JimSQueryStatus.FINISH);
    ctx.flush();
  }



  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.getCause();
    ctx.channel().close();
  }

}

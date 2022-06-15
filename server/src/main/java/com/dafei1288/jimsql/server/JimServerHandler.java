package com.dafei1288.jimsql.server;

import com.dafei1288.jimsql.common.JimSessionStatus;
import com.dafei1288.jimsql.common.RowData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JimServerHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {

    System.out.println(ctx.name());
    ctx.writeAndFlush(JimSessionStatus.BEGIN);
    ctx.flush();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    super.channelRead(ctx,msg);

    String sql = msg.toString();
    System.out.println(" sql will run : " + sql);
    if(sql.contains("select")){
      List<RowData> datas = new ArrayList<>();
      for(int i = 0;i<10;i++){
        RowData rowData = new RowData();
        rowData.setNext(true);
        if(i==99){
          rowData.setNext(false);
        }
        Map<String,Object> map = new HashMap<>();
        map.put("id","id"+i);
        map.put("name","name"+i);
        map.put("age",i);
        rowData.setDatas(map);
        ctx.writeAndFlush(rowData);
      }



    }else{
      ctx.writeAndFlush(JimSessionStatus.OK);
    }
    ctx.flush();
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//    super.channelReadComplete(ctx);
    ctx.writeAndFlush(JimSessionStatus.FINISH);
    ctx.flush();
  }



  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.getCause();
    ctx.channel().close();
  }

}

package com.dafei1288.jimsql.server.plan.physical;

import com.dafei1288.jimsql.server.plan.logical.LogicalPlan;
import com.dafei1288.jimsql.server.plan.logical.QueryLogicalPlan;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;

public interface PhysicalPlan {
  void setLogicalPlan(LogicalPlan LogicalPlan);
  LogicalPlan getLogicalPlan();

  void proxyWrite(ChannelHandlerContext ctx) throws IOException;
}

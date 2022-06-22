package com.dafei1288.jimsql.server.plan.logical;

import com.dafei1288.jimsql.server.plan.physical.PhysicalPlan;

public interface LogicalPlan {

  PhysicalPlan transform();

}

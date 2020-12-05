/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.planner.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Rule that converts a ProjectPrel followed by a FilterPrel to a FletcherFilterProjectPrel.
 */
public class FletcherFilterProjectPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new FletcherFilterProjectPrule();

  private FletcherFilterProjectPrule() {
    super(operand(ProjectPrel.class, operand(FilterPrel.class, any())), "FletcherFilterProjectPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectPrel project = (ProjectPrel) call.rel(0);
    final FilterPrel  filter = (FilterPrel) call.rel(1);

    // (RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps, RelDataType rowType)
    call.transformTo(new FletcherFilterProjectPrel(project.getCluster(), filter.getInput().getTraitSet(), filter.getInput(), project.getChildExps(), project.getRowType()));
  }

}

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
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.logical.RelOptHelper;

/**
 * Rule that converts a FilterPrel to a FletcherFilterPrel.
 */
public class FletcherFilterPrule extends Prule {
  public static final RelOptRule INSTANCE = new FletcherFilterPrule();

  private FletcherFilterPrule() {
    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(RelNode.class)), "FletcherFilterPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterPrel  filter = (FilterPrel) call.rel(0);
    final RelNode input = filter.getInput();

    call.transformTo(new FletcherFilterPrel(filter.getCluster(), input.getTraitSet(), input, filter.getCondition()));
  }


  private class Subset extends SubsetTransformer<FilterPrel, RuntimeException> {

    public Subset(RelOptRuleCall call) {
      super(call);
    }

    @Override
    public RelNode convertChild(FilterPrel filter, RelNode rel) {
      return new FletcherFilterPrel(filter.getCluster(), rel.getTraitSet(), rel, filter.getCondition());
    }

  }
}

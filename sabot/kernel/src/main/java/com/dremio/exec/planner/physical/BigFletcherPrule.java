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
import org.apache.calcite.rel.InvalidRelException;

/**
 * Rule that targets Fabian's targeted physical operators for big parquet files and converts them to a FletcherPrel.
 */
public class BigFletcherPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new BigFletcherPrule();

  private BigFletcherPrule() {
    super(operand(
      ProjectPrel.class, operand(
        StreamAggPrel.class, operand(
          UnionExchangePrel.class, operand(
            StreamAggPrel.class, operand(
              ProjectPrel.class, operand(
                FilterPrel.class, any())))))
    ), "FletcherPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectPrel topProject = (ProjectPrel) call.rel(0);
    final StreamAggPrel topStreamAgg = (StreamAggPrel) call.rel(1);
    final UnionExchangePrel unionExchange = (UnionExchangePrel) call.rel(2);
    final StreamAggPrel bottomStreamAgg = (StreamAggPrel) call.rel(3);
    final ProjectPrel bottomProject = (ProjectPrel) call.rel(4);
    final FilterPrel  filter = (FilterPrel) call.rel(5);

    // (RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps, RelDataType rowType)
    final FletcherFilterProjectPrel fletcher = new FletcherFilterProjectPrel(topProject.getCluster(), filter.getInput().getTraitSet(), filter.getInput(), topProject.getChildExps(), topProject.getRowType());

    try {
      // (RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls, OperatorPhase phase)
      final StreamAggPrel newAggregate = StreamAggPrel.create(topStreamAgg.getCluster(), topStreamAgg.getTraitSet(), fletcher, false, topStreamAgg.getGroupSet(), topStreamAgg.getGroupSets(), topStreamAgg.getAggCallList(), topStreamAgg.getOperatorPhase());
      call.transformTo(newAggregate);
    } catch (InvalidRelException e) {
      e.printStackTrace();
    }
  }

}

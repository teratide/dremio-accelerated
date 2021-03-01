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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.logical.RelOptHelper;

/**
 * Rule that converts a FilterPrel to a FletcherFilterPrel.
 */
public class FletcherFilterPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new FletcherFilterPrule();

  // The regular expression on which the FPGA kernel can match
  private String regex = "'Blue.*'";

  // Match on any filter prel
  private FletcherFilterPrule() {
    // More specific rules could be added to match specific filter conditions
    // in order to offload the computation to the correct Fletcher kernel
    super(RelOptHelper.any(FilterPrel.class), "FletcherFilterPrule");
  }

  // Check if the filter condition is or can be written to the right regex
  @Override
  public boolean matches(RelOptRuleCall call) {
    final FilterPrel filter = (FilterPrel) call.rel(0);
    final RexCall condition = (RexCall) filter.getCondition();
    final String inputCondition = condition.getOperands().get(1).toString();

    if (inputCondition.equals(regex) || convertToRegex(inputCondition).equals(regex)) {
      return true;
    } else {
      return false;
    }
  }

  private String convertToRegex(String sqlCondition) {
    String regexCondition = sqlCondition.replace("%", ".*");
    return regexCondition;
  }

  // If this matches, convert to the fletcher filter operator
  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterPrel filter = (FilterPrel) call.rel(0);

    final RexCall condition = (RexCall) filter.getCondition();
    final RexInputRef col = (RexInputRef) condition.getOperands().get(0);

    final RexLiteral conditionLiteral = (RexLiteral) condition.getOperands().get(1);
    final RexLiteral regexLiteral = RexLiteral.fromJdbcString(conditionLiteral.getType(), conditionLiteral.getTypeName(), regex);

    final List<RexNode> regexOps = Arrays.asList(col, regexLiteral);
    final RexCall regexCondition = condition.clone(condition.getType(), regexOps);

    // Transform into a fletcher operator with the corresponding regex condition
    call.transformTo(new FletcherFilterPrel(filter.getCluster(), filter.getInput().getTraitSet(), filter.getInput(), regexCondition));
  }

}

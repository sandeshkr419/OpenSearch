/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;

/**
 * Volcano CBO rule that splits an {@link OpenSearchAggregate} into
 * PARTIAL + FINAL when the input is partitioned.
 *
 * <p>Requests SINGLETON distribution on the partial output, letting Volcano's
 * trait enforcement (via {@code ExpandConversionRule} + {@code OpenSearchDistributionTraitDef})
 * automatically insert an {@code OpenSearchExchangeReducer}.
 *
 * <p>The aggregate calls are kept unchanged in both PARTIAL and FINAL — the backend's
 * {@code force_aggregate_mode} in Rust handles the actual mode forcing at execution time.
 * Schema differences between partial state and final output are resolved by
 * {@code intermediateFields} declarations on {@code AggregateCapability} and
 * {@code fixIntermediateInputTypes} in the fragment convertor.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateSplitRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchAggregateSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateSplitRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        return aggregate.getMode() == AggregateMode.SINGLE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);

        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.PARTIAL,
            aggregate.getViableBackends()
        );

        RelTraitSet singletonTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        RelNode gathered = convert(partial, singletonTraits);

        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            singletonTraits,
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            AggregateMode.FINAL,
            aggregate.getViableBackends()
        );

        call.transformTo(finalAggregate);
    }
}

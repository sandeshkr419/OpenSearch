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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.AggregateDecomposition;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Volcano CBO rule that splits an {@link OpenSearchAggregate} into
 * PARTIAL + FINAL when the input is partitioned.
 *
 * <p>Requests SINGLETON distribution on the partial output, letting Volcano's
 * trait enforcement insert an {@code OpenSearchExchangeReducer}.
 *
 * <p>Uses {@link AggregateDecomposition} from the backend's capability to expand
 * partial calls and build the final expression (e.g. AVG → SUM+COUNT partial,
 * sum/count final).
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
        RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

        String backend = aggregate.getViableBackends().getFirst();

        // Build PARTIAL aggCalls, expanding any decomposed functions.
        List<AggregateCall> partialCalls = new ArrayList<>();
        List<AggregateDecomposition> decompositions = new ArrayList<>();
        List<Integer> partialStartIndex = new ArrayList<>();

        for (AggregateCall origCall : aggregate.getAggCallList()) {
            AggregateFunction func = AggregateFunction.fromAggregateCall(origCall);
            AggregateDecomposition decomp = func != null ? context.getCapabilityRegistry().getDecomposition(backend, func) : null;
            partialStartIndex.add(partialCalls.size());
            if (decomp != null) {
                decompositions.add(decomp);
                partialCalls.addAll(decomp.partialCalls(origCall, child));
            } else {
                decompositions.add(null);
                partialCalls.add(origCall);
            }
        }

        // Partial aggregate: runs on each shard
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            partialCalls,
            AggregateMode.PARTIAL,
            aggregate.getViableBackends()
        );

        // Request SINGLETON distribution — Volcano inserts Exchange automatically
        RelTraitSet singletonTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        RelNode gathered = convert(partial, singletonTraits);

        int groupCount = aggregate.getGroupSet().cardinality();

        // FINAL aggCalls: remap each partial call to reference its column in gathered output
        List<AggregateCall> finalAggCalls = new ArrayList<>();
        for (int pi = 0; pi < partialCalls.size(); pi++) {
            AggregateCall pc = partialCalls.get(pi);
            finalAggCalls.add(pc.adaptTo(gathered, List.of(groupCount + pi), pc.filterArg, groupCount, aggregate.getGroupCount()));
        }
        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            singletonTraits,
            gathered,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            finalAggCalls,
            AggregateMode.FINAL,
            aggregate.getViableBackends()
        );

        // If no decompositions, the final aggregate is the result directly
        if (decompositions.stream().allMatch(d -> d == null)) {
            call.transformTo(finalAggregate);
            return;
        }

        // With decomposition: wrap FINAL in a Project applying finalExpression() per original call
        List<RexNode> projectExprs = new ArrayList<>();
        List<String> projectNames = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            projectExprs.add(rexBuilder.makeInputRef(finalAggregate, i));
            projectNames.add(finalAggregate.getRowType().getFieldList().get(i).getName());
        }
        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            AggregateCall origCall = aggregate.getAggCallList().get(i);
            AggregateDecomposition decomp = decompositions.get(i);
            int startIdx = partialStartIndex.get(i);
            int endIdx = (i + 1 < partialStartIndex.size()) ? partialStartIndex.get(i + 1) : partialCalls.size();
            if (decomp != null) {
                List<RexNode> partialRefs = new ArrayList<>();
                for (int pi = startIdx; pi < endIdx; pi++) {
                    partialRefs.add(rexBuilder.makeInputRef(finalAggregate, groupCount + pi));
                }
                RexNode expr = decomp.finalExpression(rexBuilder, partialRefs);
                // Cast to match the original aggregate's output type (preserves nullability)
                RelDataType origType = aggregate.getRowType().getFieldList().get(groupCount + i).getType();
                if (!expr.getType().equals(origType)) {
                    expr = rexBuilder.makeCast(origType, expr);
                }
                projectExprs.add(expr);
            } else {
                projectExprs.add(rexBuilder.makeInputRef(finalAggregate, groupCount + startIdx));
            }
            projectNames.add(origCall.name != null ? origCall.name : "expr$" + i);
        }
        call.transformTo(
            new OpenSearchProject(
                aggregate.getCluster(),
                singletonTraits,
                finalAggregate,
                projectExprs,
                aggregate.getRowType(),
                aggregate.getViableBackends()
            )
        );
    }
}

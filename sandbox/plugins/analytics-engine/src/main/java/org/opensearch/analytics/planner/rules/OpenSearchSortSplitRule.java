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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.ExecutionMode;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

import java.math.BigDecimal;

/**
 * Volcano CBO rule that splits a SINGLE-mode {@link OpenSearchSort} with non-empty
 * collation into a top-K shape. Registers two alternatives and lets Volcano pick
 * the cheaper for the actual data shape:
 *
 * <ul>
 *   <li><b>Alternative A — SINGLE on already-gathered input.</b> Wins for 1-shard
 *       SHARD+SINGLETON scans and gathered Aggregate / Join / Union / RexOver Project
 *       output. Volcano inserts an ER beneath when the child is RANDOM and
 *       Alternative B doesn't apply (no fetch).</li>
 *   <li><b>Alternative B — PARTIAL ← ER ← FINAL.</b> Each shard runs a local Sort
 *       with {@code fetch = offset + fetch} and {@code offset = null}; the
 *       coordinator merges the bounded streams and applies the original
 *       {@code offset, fetch}. Coordinator memory becomes
 *       O({@code shardCount × (offset + fetch)}). Only registered when
 *       {@code sort.fetch != null}.</li>
 * </ul>
 *
 * <p>Pure-LIMIT Sorts (empty collation) are skipped — partition-local fetch via the
 * existing trait machinery is correct.
 *
 * @opensearch.internal
 */
public class OpenSearchSortSplitRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchSortSplitRule(PlannerContext context) {
        super(operand(OpenSearchSort.class, operand(RelNode.class, any())), "OpenSearchSortSplitRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        if (sort.getMode() != ExecutionMode.SINGLE) return false;
        return sort.getCollation().getFieldCollations().isEmpty() == false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        RelNode child = call.rel(1);

        // Alternative A — SINGLE on already-gathered input.
        RelTraitSet singletonTraits = sort.getTraitSet().replace(distTraitDef.coordSingleton());
        RelNode singletonChild = convert(child, singletonTraits);
        OpenSearchSort singleOnSingleton = new OpenSearchSort(
            sort.getCluster(),
            singletonTraits,
            singletonChild,
            sort.getCollation(),
            sort.offset,
            sort.fetch,
            ExecutionMode.SINGLE,
            sort.getViableBackends()
        );

        // Alternative B — PARTIAL ← ER ← FINAL. Only viable when fetch is bounded.
        RexNode partialFetch = computePartialFetch(sort);
        if (partialFetch == null) {
            call.transformTo(singleOnSingleton);
            return;
        }

        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchSort partial = new OpenSearchSort(
            sort.getCluster(),
            partialTraits,
            child,
            sort.getCollation(),
            /* offset */ null,
            /* fetch  */ partialFetch,
            ExecutionMode.PARTIAL,
            sort.getViableBackends()
        );
        RelTraitSet finalTraits = partial.getTraitSet().replace(distTraitDef.coordSingleton());
        RelNode gathered = convert(partial, finalTraits);
        OpenSearchSort finalSort = new OpenSearchSort(
            sort.getCluster(),
            finalTraits,
            gathered,
            sort.getCollation(),
            sort.offset,
            sort.fetch,
            ExecutionMode.FINAL,
            sort.getViableBackends()
        );

        call.getPlanner().ensureRegistered(singleOnSingleton, sort);
        call.transformTo(finalSort);
    }

    /**
     * Per-shard fetch bound = {@code offset + fetch}, preserving the original literal
     * type. Returns {@code null} (skipping Alternative B) when no bound exists, an
     * operand isn't a literal, or {@code offset + fetch} overflows int.
     */
    private static RexNode computePartialFetch(OpenSearchSort sort) {
        if (sort.fetch == null) return null;
        if (!(sort.fetch instanceof RexLiteral fetchLit)) return null;
        if (sort.offset == null) return sort.fetch;
        if (!(sort.offset instanceof RexLiteral offsetLit)) return null;

        long fetchVal = RexLiteral.intValue(fetchLit);
        long offsetVal = RexLiteral.intValue(offsetLit);
        long sum;
        try {
            sum = Math.addExact(fetchVal, offsetVal);
        } catch (ArithmeticException overflow) {
            return null;
        }
        if (sum > Integer.MAX_VALUE) return null;

        RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(sum), sort.fetch.getType());
    }
}

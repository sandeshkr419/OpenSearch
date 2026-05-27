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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.ShardBucketHint;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-CBO execution-hint rule. Matches {@code OpenSearchSort + (optional Project) +
 * OpenSearchAggregate(SINGLE)} and attaches a {@link ShardBucketHint} to the aggregate;
 * the hint is consumed at split time by {@link OpenSearchAggregateSplitRule}.
 *
 * <p>Each shard ships {@code ceil(max(LIMIT, 10) * factor) + 10} buckets, mirroring native
 * OpenSearch terms-aggregation {@code shard_size}. {@code factor} is the min of the
 * {@code index.analytics.shard_bucket_oversampling_factor} setting across involved tables;
 * any participating index with {@code factor == 0} disables the rewrite for the whole query.
 *
 * <p>The hint's {@code sortExprs} carry per-collation-field RexNodes evaluated at the shard.
 * Three sources: pass-through column ref (SUM/MIN/MAX/COUNT), the recompose Project's
 * RexNode (AVG/STDDEV/VAR), or {@link AggregateFunction#finalizeOperator} (engine-native
 * merge — APPROX_COUNT_DISTINCT). Approximate: a group missing from every shard's
 * top-{@code shardSize} cannot reach the coordinator.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateShardBucketRule extends RelOptRule {

    /** PPL {@code head} default; used as coord-limit floor when no LIMIT is set. */
    private static final long DEFAULT_COORD_LIMIT = 10L;

    public OpenSearchAggregateShardBucketRule() {
        super(operand(OpenSearchSort.class, any()), "OpenSearchAggregateShardBucketRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        if (sort.getCollation().getFieldCollations().isEmpty()) return false;
        OpenSearchAggregate aggregate = aggregateBelow(sort);
        if (aggregate == null) return false;
        if (aggregate.getMode() != AggregateMode.SINGLE) return false;
        if (aggregate.getShardBucketHint() != null) return false; // idempotent
        // Skip join/union subtrees — per-shard top-K is only valid for single-table scans.
        if (hasMultiInput(aggregate)) return false;
        return true;
    }

    /** True when any node in the subtree has more than one input (join/union). */
    private static boolean hasMultiInput(RelNode node) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        if (current.getInputs().size() > 1) return true;
        for (RelNode input : current.getInputs()) {
            if (hasMultiInput(input)) return true;
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchSort outerSort = call.rel(0);
        Project recomposeProject = projectBelow(outerSort);
        OpenSearchAggregate aggregate = aggregateBelow(outerSort);
        if (aggregate == null) return;

        double factor = resolveFactor(aggregate);
        if (factor == 0.0) return;
        // Single-shard topology never splits, so the hint would never be consumed.
        if (allTablesAreSingleShard(aggregate)) return;

        long coordLimit = (outerSort.fetch instanceof RexLiteral lit) ? RexLiteral.intValue(lit) : DEFAULT_COORD_LIMIT;
        long shardSize = (long) Math.ceil(Math.max(coordLimit, DEFAULT_COORD_LIMIT) * factor) + DEFAULT_COORD_LIMIT;
        if (shardSize > Integer.MAX_VALUE) return;
        if (shardSize <= coordLimit) return; // defensive: wouldn't bound below the user's request

        List<RexNode> sortExprs = buildSortExprs(outerSort.getCollation(), aggregate, recomposeProject);
        if (sortExprs == null) return; // unsupported sort-key shape — fall back to gather-all

        // Synthetic dense [0..N) collation parallels sortExprs positions; directions mirror the outer Sort.
        RelCollation synthCollation = denseCollationOf(outerSort.getCollation());

        ShardBucketHint hint = new ShardBucketHint((int) shardSize, synthCollation, sortExprs);
        OpenSearchAggregate hinted = aggregate.withShardBucketHint(hint);

        // Re-attach Project (if any) on top of the hinted aggregate, then the outer Sort.
        RelNode rebuiltChild = recomposeProject == null
            ? hinted
            : recomposeProject.copy(recomposeProject.getTraitSet(), List.of((RelNode) hinted));
        RelNode rebuilt = outerSort.copy(
            outerSort.getTraitSet(),
            rebuiltChild,
            outerSort.getCollation(),
            outerSort.offset,
            outerSort.fetch
        );
        call.transformTo(rebuilt);
    }

    /**
     * Translates each {@link RelFieldCollation} in {@code outerCollation} into a {@code RexNode}
     * that the shard-side runtime evaluates over the shard aggregate's output. Returns
     * {@code null} when any field can't be expressed (e.g. unrecognised aggregate).
     */
    private List<RexNode> buildSortExprs(RelCollation outerCollation, OpenSearchAggregate aggregate, Project recomposeProject) {
        RexBuilder rb = aggregate.getCluster().getRexBuilder();
        RelDataTypeFactory tf = aggregate.getCluster().getTypeFactory();
        int groupCount = aggregate.getGroupSet().cardinality();
        List<RexNode> exprs = new ArrayList<>(outerCollation.getFieldCollations().size());

        for (RelFieldCollation fc : outerCollation.getFieldCollations()) {
            int outerFieldIdx = fc.getFieldIndex();
            RexNode expr = sortKeyForOuterField(outerFieldIdx, aggregate, recomposeProject, groupCount, rb, tf);
            if (expr == null) return null;
            exprs.add(expr);
        }
        return exprs;
    }

    /** Sort key for one outer-Sort collation field, expressed against the shard aggregate's output schema. */
    private RexNode sortKeyForOuterField(
        int outerFieldIdx,
        OpenSearchAggregate aggregate,
        Project recomposeProject,
        int groupCount,
        RexBuilder rb,
        RelDataTypeFactory tf
    ) {
        RexNode logicalExpr = recomposeProject == null
            ? rb.makeInputRef(aggregate.getRowType().getFieldList().get(outerFieldIdx).getType(), outerFieldIdx)
            : recomposeProject.getProjects().get(outerFieldIdx);
        return logicalExpr.accept(new EngineNativeMergeRewriter(aggregate, groupCount, rb, tf));
    }

    /** Replaces RexInputRefs to engine-native-merge aggCall outputs with {@code finalizeOperator(state)}; passes everything else through. */
    private static final class EngineNativeMergeRewriter extends RexShuttle {
        private final OpenSearchAggregate aggregate;
        private final int groupCount;
        private final RexBuilder rb;
        private final RelDataTypeFactory tf;

        EngineNativeMergeRewriter(OpenSearchAggregate aggregate, int groupCount, RexBuilder rb, RelDataTypeFactory tf) {
            this.aggregate = aggregate;
            this.groupCount = groupCount;
            this.rb = rb;
            this.tf = tf;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            int idx = inputRef.getIndex();
            if (idx < groupCount) return inputRef;
            int aggCallIdx = idx - groupCount;
            if (aggCallIdx >= aggregate.getAggCallList().size()) return inputRef;
            AggregateCall call = aggregate.getAggCallList().get(aggCallIdx);
            AggregateFunction fn = AggregateFunction.fromSqlAggFunction(call.getAggregation());
            if (fn == null) return inputRef;
            List<AggregateFunction.IntermediateField> fields = fn.intermediateFields();
            if (fields == null || fields.size() != 1) return inputRef;
            AggregateFunction.IntermediateField field = fields.get(0);
            if (field.reducer() != fn) return inputRef; // function-swap (COUNT→SUM): scalar pass-through
            // Engine-native merge: wrap a state-typed input ref in the aggregate's finalize call.
            List<RelDataType> argTypes = call.getArgList()
                .stream()
                .map(i -> aggregate.getInput().getRowType().getFieldList().get(i).getType())
                .toList();
            RelDataType stateType = field.typeResolver().resolve(argTypes, tf);
            RexNode stateRef = rb.makeInputRef(stateType, idx);
            return fn.finalizeOperator().<RexNode>map(op -> rb.makeCall(op, List.of(stateRef))).orElse(stateRef);
        }
    }

    /** Dense {@code [0..N)} collation; directions copied from {@code outer}. */
    private static RelCollation denseCollationOf(RelCollation outer) {
        List<RelFieldCollation> fcs = new ArrayList<>(outer.getFieldCollations().size());
        for (int i = 0; i < outer.getFieldCollations().size(); i++) {
            RelFieldCollation original = outer.getFieldCollations().get(i);
            fcs.add(new RelFieldCollation(i, original.direction, original.nullDirection));
        }
        return RelCollations.of(fcs);
    }

    /** True when every {@link OpenSearchTableScan} under {@code subtree} has a single shard. */
    private boolean allTablesAreSingleShard(RelNode subtree) {
        boolean[] anyMultiShard = { false };
        boolean[] anyTable = { false };
        visitScans(subtree, scan -> {
            anyTable[0] = true;
            for (int i = 0; i < scan.getTraitSet().size(); i++) {
                if (scan.getTraitSet().getTrait(i) instanceof OpenSearchDistribution dist
                        && dist.getShardCount() != null && dist.getShardCount() > 1) {
                    anyMultiShard[0] = true;
                }
            }
        });
        return anyTable[0] && !anyMultiShard[0];
    }


    /** {@link Project} immediately below {@code sort}, or {@code null}. */
    private static Project projectBelow(OpenSearchSort sort) {
        RelNode child = RelNodeUtils.unwrapHep(sort.getInput());
        return (child instanceof Project p) ? p : null;
    }

    /** OpenSearchAggregate directly below {@code sort}, or below at most one {@link Project} layer. */
    private static OpenSearchAggregate aggregateBelow(OpenSearchSort sort) {
        RelNode child = RelNodeUtils.unwrapHep(sort.getInput());
        if (child instanceof Project p) {
            child = RelNodeUtils.unwrapHep(p.getInput());
        }
        return (child instanceof OpenSearchAggregate aggregate) ? aggregate : null;
    }

    /** Min {@code shardBucketFactor} across scans; {@code 0.0} disables. */
    private double resolveFactor(RelNode subtree) {
        double[] state = { Double.MAX_VALUE };
        boolean[] any = { false };
        visitScans(subtree, scan -> {
            double f = scan.getShardBucketFactor();
            if (f < state[0]) state[0] = f;
            any[0] = true;
        });
        return any[0] ? state[0] : 0.0;
    }

    /** True when every scan has a single shard (CBO won't split). */

    private static void visitScans(RelNode node, java.util.function.Consumer<OpenSearchTableScan> visitor) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        if (current instanceof OpenSearchTableScan scan) {
            visitor.accept(scan);
            return;
        }
        for (RelNode input : current.getInputs()) {
            visitScans(input, visitor);
        }
    }
}

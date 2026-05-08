/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Builds a {@link QueryDAG} from the CBO output by cutting at exchange boundaries.
 *
 * <p>When cutting at an aggregate exchange, the shard fragment is decomposed:
 * AVG(x) → COUNT(x) + SUM(x) so the Calcite row type matches DataFusion's partial output.
 * The coordinator's {@link OpenSearchStageInputScan} row type is derived from
 * {@link AggregateFunction#getIntermediateFields()} for functions where the Calcite type
 * differs from DataFusion's partial output (e.g. DC → VARBINARY).
 *
 * <p>The coordinator's FINAL aggregate is also decomposed: each aggCall's arg is rewritten
 * to reference the correct partial state column. DC is kept unchanged — DataFusion reads
 * the HLL sketch by accumulator position, not by arg.
 *
 * @opensearch.internal
 */
public class DAGBuilder {

    private DAGBuilder() {}

    public static QueryDAG build(RelNode cboOutput, CapabilityRegistry registry, ClusterService clusterService) {
        int[] counter = { 0 };
        List<Stage> childStages = new ArrayList<>();

        RelNode rootFragment;
        if (cboOutput instanceof OpenSearchExchangeReducer reducer) {
            rootFragment = cutSingleton(reducer, counter, childStages, clusterService);
        } else {
            rootFragment = sever(cboOutput, counter, childStages, registry, clusterService);
        }

        ExchangeSinkProvider sinkProvider = null;
        if (!childStages.isEmpty()) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
                registry,
                ((OpenSearchRelNode) cboOutput).getViableBackends()
            );
            sinkProvider = registry.getBackend(reduceViable.getFirst()).getExchangeSinkProvider();
        }

        TargetResolver rootTargetResolver = childStages.isEmpty() ? new ShardTargetResolver(rootFragment, clusterService) : null;

        Stage rootStage = new Stage(counter[0]++, rootFragment, childStages, null, sinkProvider, rootTargetResolver);
        return new QueryDAG(UUID.randomUUID().toString(), rootStage);
    }

    private static RelNode sever(
        RelNode node,
        int[] counter,
        List<Stage> childStages,
        CapabilityRegistry registry,
        ClusterService clusterService
    ) {
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            if (input instanceof OpenSearchExchangeReducer reducer) {
                newInputs.add(cutSingleton(reducer, counter, childStages, clusterService));
            } else {
                newInputs.add(sever(input, counter, childStages, registry, clusterService));
            }
        }
        if (node.getInputs().isEmpty()) return node;
        boolean changed = false;
        for (int i = 0; i < newInputs.size(); i++) {
            if (newInputs.get(i) != node.getInputs().get(i)) {
                changed = true;
                break;
            }
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static RelNode cutSingleton(
        OpenSearchExchangeReducer reducer,
        int[] counter,
        List<Stage> parentChildStages,
        ClusterService clusterService
    ) {
        List<Stage> grandchildren = new ArrayList<>();
        RelNode childFragment = reducer.getInput();

        // Decompose shard fragment: AVG → COUNT+SUM so Calcite row type matches DataFusion partial output
        RelNode decomposedChildFragment = decomposePartialFragment(childFragment);

        int childStageId = counter[0]++;
        parentChildStages.add(
            new Stage(
                childStageId,
                decomposedChildFragment,
                grandchildren,
                ExchangeInfo.singleton(),
                null,
                new ShardTargetResolver(childFragment, clusterService)
            )
        );

        // StageInputScan row type: use intermediateFields for DC (VARBINARY), Calcite type for others
        RelDataType stageInputRowType = intermediateRowType(decomposedChildFragment, reducer.getCluster().getTypeFactory());

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            reducer.getCluster(),
            reducer.getTraitSet(),
            childStageId,
            stageInputRowType,
            reducer.getViableBackends()
        );
        return new OpenSearchExchangeReducer(reducer.getCluster(), reducer.getTraitSet(), stageInput, reducer.getViableBackends());
    }

    /**
     * Expands AVG → COUNT+SUM in PARTIAL fragment so the Calcite row type matches
     * DataFusion's actual partial output schema. All other functions unchanged.
     */
    private static RelNode decomposePartialFragment(RelNode node) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL) {
            boolean needsDecomposition = agg.getAggCallList()
                .stream()
                .map(AggregateFunction::fromAggregateCall)
                .anyMatch(f -> f != null && f.getIntermediateFields() != null && f.getIntermediateFields().size() > 1);
            if (!needsDecomposition) return node;

            RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();
            int groupCount = agg.getGroupSet().cardinality();
            List<AggregateCall> newCalls = new ArrayList<>();
            for (AggregateCall call : agg.getAggCallList()) {
                AggregateFunction func = AggregateFunction.fromAggregateCall(call);
                var iFields = func != null ? func.getIntermediateFields() : null;
                if (iFields != null && iFields.size() > 1) {
                    // AVG → COUNT(x) + SUM(x)
                    var inputType = agg.getInput().getRowType().getFieldList().get(call.getArgList().get(0)).getType();
                    newCalls.add(
                        AggregateCall.create(
                            SqlStdOperatorTable.COUNT,
                            false,
                            false,
                            false,
                            List.of(),
                            call.getArgList(),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            typeFactory.createSqlType(SqlTypeName.BIGINT),
                            call.name + iFields.get(0).getName()
                        )
                    );
                    var sumBinding = new org.apache.calcite.rel.core.Aggregate.AggCallBinding(
                        typeFactory,
                        SqlStdOperatorTable.SUM,
                        List.of(inputType),
                        groupCount,
                        false
                    );
                    newCalls.add(
                        AggregateCall.create(
                            SqlStdOperatorTable.SUM,
                            false,
                            false,
                            false,
                            List.of(),
                            call.getArgList(),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            SqlStdOperatorTable.SUM.inferReturnType(sumBinding),
                            call.name + iFields.get(1).getName()
                        )
                    );
                } else {
                    newCalls.add(call);
                }
            }
            return new OpenSearchAggregate(
                agg.getCluster(),
                agg.getTraitSet(),
                agg.getInput(),
                agg.getGroupSet(),
                agg.getGroupSets(),
                newCalls,
                AggregateMode.PARTIAL,
                agg.getViableBackends()
            );
        }
        if (node.getInputs().size() == 1) {
            RelNode fixed = decomposePartialFragment(node.getInputs().get(0));
            return fixed != node.getInputs().get(0) ? node.copy(node.getTraitSet(), List.of(fixed)) : node;
        }
        return node;
    }

    /**
     * StageInputScan row type: VARBINARY for DC (DataFusion emits Binary sketch),
     * Calcite row type for all other functions.
     */
    private static RelDataType intermediateRowType(RelNode partialFragment, RelDataTypeFactory typeFactory) {
        OpenSearchAggregate agg = findPartialAggregate(partialFragment);
        if (agg == null) return partialFragment.getRowType();

        boolean needsOverride = agg.getAggCallList()
            .stream()
            .map(AggregateFunction::fromAggregateCall)
            .anyMatch(
                f -> f != null
                    && f.getIntermediateFields() != null
                    && f.getIntermediateFields()
                        .stream()
                        .anyMatch(iField -> iField.getFieldType().getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary)
            );
        if (!needsOverride) return agg.getRowType();

        List<RelDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        int groupCount = agg.getGroupSet().cardinality();
        for (int i = 0; i < groupCount; i++) {
            var f = agg.getRowType().getFieldList().get(i);
            types.add(f.getType());
            names.add(f.getName());
        }
        int colIdx = groupCount;
        for (AggregateCall call : agg.getAggCallList()) {
            AggregateFunction func = AggregateFunction.fromAggregateCall(call);
            var iFields = func != null ? func.getIntermediateFields() : null;
            var f = agg.getRowType().getFieldList().get(colIdx++);
            boolean hasBinary = iFields != null
                && iFields.stream()
                    .anyMatch(iField -> iField.getFieldType().getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary);
            types.add(hasBinary ? typeFactory.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE) : f.getType());
            names.add(f.getName());
        }
        return typeFactory.createStructType(types, names);
    }

    /**
     * Decomposes FINAL aggregate: rewrites each aggCall's arg to reference the correct
     * partial state column. DC is kept unchanged — DataFusion reads sketch by accumulator
     * position, not by arg (changing the arg causes a Binary→Int64 cast panic).
     * AVG (multi-field) gets SUM+SUM calls + Project(DIV).
     */
    static RelNode decomposeFinalFragment(RelNode node) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) {
            RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();
            RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
            int groupCount = agg.getGroupSet().cardinality();

            List<AggregateCall> newCalls = new ArrayList<>();
            List<java.util.function.BiFunction<RexBuilder, List<RexNode>, RexNode>> finalExprs = new ArrayList<>();
            boolean needsProject = false;

            for (AggregateCall call : agg.getAggCallList()) {
                AggregateFunction func = AggregateFunction.fromAggregateCall(call);
                var iFields = func != null ? func.getIntermediateFields() : null;
                var finalExpr = func != null ? func.getFinalExpression() : null;

                if (iFields != null && iFields.size() > 1 && finalExpr != null) {
                    // AVG: SUM each intermediate field, apply finalExpr in Project
                    for (int j = 0; j < iFields.size(); j++) {
                        int colIdx = groupCount + newCalls.size();
                        var colType = agg.getInput().getRowType().getFieldList().get(colIdx).getType();
                        var binding = new org.apache.calcite.rel.core.Aggregate.AggCallBinding(
                            typeFactory,
                            SqlStdOperatorTable.SUM,
                            List.of(colType),
                            groupCount,
                            false
                        );
                        newCalls.add(
                            AggregateCall.create(
                                SqlStdOperatorTable.SUM,
                                false,
                                false,
                                false,
                                List.of(),
                                List.of(colIdx),
                                -1,
                                null,
                                RelCollations.EMPTY,
                                SqlStdOperatorTable.SUM.inferReturnType(binding),
                                call.name + iFields.get(j).getName()
                            )
                        );
                    }
                    finalExprs.add(finalExpr);
                    needsProject = true;
                } else {
                    // DC: keep original call (DataFusion reads sketch by position, not arg)
                    // DC (hasBinary): keep original call — DataFusion reads sketch by position
                    // COUNT (single-field intermediate, finalExpr != null): use SUM to merge partial counts
                    // SUM (no intermediateFields): rewrite arg to reference partial state column
                    boolean hasBinary = iFields != null
                        && iFields.stream()
                            .anyMatch(
                                iField -> iField.getFieldType().getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary
                            );
                    int colIdx = groupCount + newCalls.size();
                    boolean isSingleFieldWithFinalExpr = iFields != null && iFields.size() == 1 && finalExpr != null;
                    if (hasBinary) {
                        newCalls.add(call); // DC: keep original — fixIntermediateInputTypes handles scan rewriting
                        finalExprs.add(null);
                    } else if (isSingleFieldWithFinalExpr) {
                        // COUNT: SUM the partial count (no Project needed — type difference is fine for DataFusion)
                        var colType = agg.getInput().getRowType().getFieldList().get(colIdx).getType();
                        var binding = new org.apache.calcite.rel.core.Aggregate.AggCallBinding(
                            typeFactory,
                            SqlStdOperatorTable.SUM,
                            List.of(colType),
                            groupCount,
                            false
                        );
                        newCalls.add(
                            AggregateCall.create(
                                SqlStdOperatorTable.SUM,
                                false,
                                false,
                                false,
                                List.of(),
                                List.of(colIdx),
                                -1,
                                null,
                                RelCollations.EMPTY,
                                SqlStdOperatorTable.SUM.inferReturnType(binding),
                                call.name
                            )
                        );
                        finalExprs.add(null); // no Project — SUM result used directly
                    } else {
                        newCalls.add(call.withArgList(List.of(colIdx))); // SUM: rewrite arg
                        finalExprs.add(null);
                    }
                }
            }

            OpenSearchAggregate newAgg = new OpenSearchAggregate(
                agg.getCluster(),
                agg.getTraitSet(),
                agg.getInput(),
                agg.getGroupSet(),
                agg.getGroupSets(),
                newCalls,
                AggregateMode.FINAL,
                agg.getViableBackends()
            );

            if (!needsProject) return newAgg;

            // Wrap in Project for AVG: apply finalExpression (DIV) over SUM results
            List<RexNode> projectExprs = new ArrayList<>();
            List<String> projectNames = new ArrayList<>();
            for (int i = 0; i < groupCount; i++) {
                projectExprs.add(rexBuilder.makeInputRef(newAgg, i));
                projectNames.add(newAgg.getRowType().getFieldList().get(i).getName());
            }
            int aggColIdx = groupCount;
            int origIdx = 0;
            for (AggregateCall origCall : agg.getAggCallList()) {
                AggregateFunction func = AggregateFunction.fromAggregateCall(origCall);
                var iFields = func != null ? func.getIntermediateFields() : null;
                var finalExpr = finalExprs.get(origIdx++);
                if (iFields != null && iFields.size() > 1 && finalExpr != null) {
                    List<RexNode> refs = new ArrayList<>();
                    for (int j = 0; j < iFields.size(); j++) {
                        refs.add(rexBuilder.makeInputRef(newAgg, aggColIdx + j));
                    }
                    var expr = finalExpr.apply(rexBuilder, refs);
                    // Cast to original aggregate's output type to satisfy LogicalProject validation
                    var origType = agg.getRowType().getFieldList().get(groupCount + origIdx - 1).getType();
                    projectExprs.add(expr.getType().equals(origType) ? expr : rexBuilder.makeCast(origType, expr));
                    aggColIdx += iFields.size();
                } else {
                    projectExprs.add(rexBuilder.makeInputRef(newAgg, aggColIdx++));
                }
                projectNames.add(
                    origCall.name != null ? origCall.name : agg.getRowType().getFieldList().get(groupCount + origIdx - 1).getName()
                );
            }
            return LogicalProject.create(newAgg, List.of(), projectExprs, agg.getRowType());
        }

        if (node.getInputs().isEmpty()) return node;
        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode fixed = decomposeFinalFragment(input);
            newInputs.add(fixed);
            if (fixed != input) changed = true;
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static OpenSearchAggregate findPartialAggregate(RelNode node) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL) return agg;
        if (node.getInputs().size() == 1) return findPartialAggregate(node.getInputs().get(0));
        return null;
    }
}

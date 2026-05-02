/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.AggregateDecomposition;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds executions for {@link StageExecutionType#COORDINATOR_REDUCE} stages —
 * those that run at the coordinator with a backend-provided {@link ExchangeSink}.
 * Creates the sink via {@link Stage#getExchangeSinkProvider()} using an
 * {@link ExchangeSinkContext} carrying the plan bytes, allocator, input
 * schema (derived from the single child stage), and downstream sink. Hands
 * the resulting sink to {@link LocalStageExecution}.
 *
 * <p>Single-sink simplification: assumes exactly one child stage. Multi-child
 * (joins, set ops) will require per-child sink routing in a follow-up.
 *
 * @opensearch.internal
 */
final class LocalStageScheduler implements StageScheduler {

    private final CapabilityRegistry capabilityRegistry;

    LocalStageScheduler(CapabilityRegistry capabilityRegistry) {
        this.capabilityRegistry = capabilityRegistry;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        ExchangeSinkProvider provider = stage.getExchangeSinkProvider();
        ExchangeSinkContext context = new ExchangeSinkContext(
            config.queryId(),
            stage.getStageId(),
            chosenBytes(stage),
            config.bufferAllocator(),
            deriveInputSchema(stage, capabilityRegistry),
            sink
        );
        ExchangeSink backendSink;
        try {
            backendSink = provider.createSink(context);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create exchange sink for stageId=" + stage.getStageId(), e);
        }
        return new LocalStageExecution(stage, backendSink, sink);
    }

    /** Picks the plan-alternative bytes bound to the stage's exchange sink provider. */
    private static byte[] chosenBytes(Stage stage) {
        assert stage.getPlanAlternatives().size() == 1 : "COORDINATOR_REDUCE stage "
            + stage.getStageId()
            + " expected exactly one plan alternative, got "
            + stage.getPlanAlternatives().size();
        return stage.getPlanAlternatives().getFirst().convertedBytes();
    }

    /**
     * Derives the backend's input Arrow schema from the single child stage's
     * fragment rowtype. Multi-child support (joins, set ops with heterogeneous
     * inputs) is deferred.
     */
    private static Schema deriveInputSchema(Stage stage, CapabilityRegistry capabilityRegistry) {
        List<Stage> children = stage.getChildStages();
        assert children.size() == 1 : "COORDINATOR_REDUCE stage "
            + stage.getStageId()
            + " expected exactly one child stage, got "
            + children.size();
        Stage child = children.getFirst();
        RelNode childFragment = child.getPlanAlternatives().isEmpty()
            ? child.getFragment()
            : child.getPlanAlternatives().getFirst().resolvedFragment();

        Aggregate agg = findAggregate(childFragment);
        if (agg == null) {
            return ArrowSchemaFromCalcite.arrowSchemaFromRowType(childFragment.getRowType());
        }

        // Determine the backend for this child stage to look up decompositions
        String backendId = child.getPlanAlternatives().isEmpty()
            ? null
            : child.getPlanAlternatives().getFirst().backendId();

        List<Field> fields = new ArrayList<>();
        int groupCount = agg.getGroupSet().cardinality();
        for (int i = 0; i < groupCount; i++) {
            RelDataTypeField f = childFragment.getRowType().getFieldList().get(i);
            fields.add(ArrowSchemaFromCalcite.fieldFromCalcite(f));
        }
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCall call = agg.getAggCallList().get(i);
            RelDataTypeField f = childFragment.getRowType().getFieldList().get(groupCount + i);
            ArrowType overrideType = resolveIntermediateArrowType(call, backendId, capabilityRegistry);
            if (overrideType != null) {
                fields.add(new Field(f.getName(), new FieldType(true, overrideType, null), null));
            } else {
                fields.add(ArrowSchemaFromCalcite.fieldFromCalcite(f));
            }
        }
        return new Schema(fields);
    }

    /**
     * Returns the intermediate Arrow type for a partial aggregate call if the
     * backend's decomposition declares one, or {@code null} to use the Calcite type.
     */
    private static ArrowType resolveIntermediateArrowType(
        AggregateCall call, String backendId, CapabilityRegistry registry
    ) {
        if (backendId == null || registry == null) return null;
        AggregateFunction func = AggregateFunction.fromSqlKind(call.getAggregation().getKind());
        // APPROX_COUNT_DISTINCT has SqlKind.COUNT — check by name when approximate
        if (func == AggregateFunction.COUNT && call.isApproximate()) {
            try { func = AggregateFunction.fromNameOrError(call.getAggregation().getName()); }
            catch (IllegalArgumentException ignored) {}
        }
        if (func == null) return null;
        AggregateDecomposition decomp = registry.getDecomposition(backendId, func);
        if (decomp == null) return null;
        List<org.apache.arrow.vector.types.pojo.ArrowType> types = decomp.intermediateArrowTypes();
        return types.isEmpty() ? null : types.get(0);
    }

    private static Aggregate findAggregate(RelNode node) {
        if (node instanceof Aggregate agg) return agg;
        for (RelNode input : node.getInputs()) {
            Aggregate found = findAggregate(input);
            if (found != null) return found;
        }
        return null;
    }
}

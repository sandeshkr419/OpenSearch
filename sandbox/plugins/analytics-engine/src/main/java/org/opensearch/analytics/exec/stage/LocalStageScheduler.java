/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.backend.AggregateExecutionMode;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
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
        byte[] planBytes = chosenBytes(stage);
        // Coordinator-reduce stages always execute in final-aggregate mode (2).
        // The shard path uses mode 1 (partial), set in DatafusionSearchExecEngine.
        ExchangeSinkContext context = new ExchangeSinkContext(
            config.queryId(),
            stage.getStageId(),
            planBytes,
            config.bufferAllocator(),
            deriveInputSchema(stage, capabilityRegistry),
            sink,
            AggregateExecutionMode.FINAL
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
     *
     * <p>TODO: this schema derivation belongs in the planner, not the scheduler.
     * The intermediate Arrow type should be determined during plan forking/resolution
     * and carried on the {@link org.opensearch.analytics.planner.dag.StagePlan} so
     * the scheduler only reads it — scheduling should not become another planner.
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

        // Determine the backend for this child stage to look up intermediate field declarations
        String backendId = child.getPlanAlternatives().isEmpty() ? null : child.getPlanAlternatives().getFirst().backendId();

        List<Field> fields = new ArrayList<>();
        int groupCount = agg.getGroupSet().cardinality();
        for (int i = 0; i < groupCount; i++) {
            RelDataTypeField f = childFragment.getRowType().getFieldList().get(i);
            fields.add(ArrowSchemaFromCalcite.fieldFromCalcite(f));
        }
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCall call = agg.getAggCallList().get(i);
            RelDataTypeField f = childFragment.getRowType().getFieldList().get(groupCount + i);
            List<Field> intermediateFields = resolveIntermediateFields(call, backendId, capabilityRegistry);
            if (intermediateFields != null) {
                for (Field iField : intermediateFields) {
                    String fieldName = iField.getName().isEmpty() ? f.getName() : f.getName() + iField.getName();
                    fields.add(new Field(fieldName, iField.getFieldType(), null));
                }
            } else {
                fields.add(ArrowSchemaFromCalcite.fieldFromCalcite(f));
            }
        }
        return new Schema(fields);
    }

    private static List<Field> resolveIntermediateFields(AggregateCall call, String backendId, CapabilityRegistry registry) {
        if (backendId == null) return null;
        AggregateFunction func = AggregateFunction.fromAggregateCall(call);
        if (func == null) return null;
        return registry.getIntermediateFields(backendId, func);
    }

    private static Aggregate findAggregate(RelNode node) {
        if (node instanceof Aggregate agg) return agg;
        // Only unwrap single-input schema-transparent nodes (Project, Sort) to reach the aggregate
        if (node.getInputs().size() == 1) return findAggregate(node.getInputs().get(0));
        return null;
    }
}

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
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds executions for {@link StageExecutionType#COORDINATOR_REDUCE} stages.
 *
 * <p>The streaming table schema is derived from the child stage's decomposed fragment
 * (set by {@link org.opensearch.analytics.planner.dag.DAGBuilder}). For DC, the schema
 * uses Binary from {@link AggregateFunction#getIntermediateFields()} since DataFusion's
 * partial DC emits an HLL sketch (Binary), not BIGINT NOT NULL.
 *
 * @opensearch.internal
 */
final class LocalStageScheduler implements StageScheduler {

    LocalStageScheduler() {}

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        ExchangeSinkProvider provider = stage.getExchangeSinkProvider();
        ExchangeSinkContext context = new ExchangeSinkContext(
            config.queryId(),
            stage.getStageId(),
            chosenBytes(stage),
            config.bufferAllocator(),
            buildChildInputs(stage),
            sink
        );

        FragmentInstructionHandlerFactory factory = stage.getInstructionHandlerFactory();
        BackendExecutionContext backendContext = null;
        if (factory != null) {
            for (InstructionNode node : stage.getPlanAlternatives().getFirst().instructions()) {
                FragmentInstructionHandler handler = factory.createHandler(node);
                backendContext = handler.apply(node, context, backendContext);
            }
        }

        ExchangeSink backendSink;
        try {
            backendSink = provider.createSink(context, backendContext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create exchange sink for stageId=" + stage.getStageId(), e);
        }
        return new LocalStageExecution(stage, backendSink, sink);
    }

    private static byte[] chosenBytes(Stage stage) {
        assert stage.getPlanAlternatives().size() == 1 : "COORDINATOR_REDUCE stage "
            + stage.getStageId()
            + " expected exactly one plan alternative, got "
            + stage.getPlanAlternatives().size();
        return stage.getPlanAlternatives().getFirst().convertedBytes();
    }

    private List<ExchangeSinkContext.ChildInput> buildChildInputs(Stage stage) {
        List<Stage> children = stage.getChildStages();
        if (children.isEmpty()) {
            throw new IllegalStateException(
                "COORDINATOR_REDUCE stage " + stage.getStageId() + " expected at least one child stage, got zero"
            );
        }
        List<ExchangeSinkContext.ChildInput> inputs = new ArrayList<>(children.size());
        for (Stage child : children) {
            inputs.add(new ExchangeSinkContext.ChildInput(child.getStageId(), deriveChildSchema(child)));
        }
        return inputs;
    }

    /**
     * Derives the streaming table schema from the decomposed shard fragment.
     * Uses {@link AggregateFunction#getIntermediateFields()} for DC (Binary sketch)
     * since DataFusion's partial DC emits Binary, not BIGINT NOT NULL.
     */
    private Schema deriveChildSchema(Stage child) {
        // Use resolvedFragment (set by PlanForker after BackendPlanAdapter adaptation)
        // so the DC call has the adapted APPROX_DISTINCT function name for lookup.
        var fragment = child.getPlanAlternatives().isEmpty()
            ? child.getFragment()
            : child.getPlanAlternatives().getFirst().resolvedFragment();
        var rowType = fragment.getRowType();

        var agg = findAggregate(fragment);
        if (agg == null) return ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType);

        // Override only for functions with Binary intermediate fields (DC)
        boolean needsOverride = agg.getAggCallList().stream()
            .map(AggregateFunction::fromAggregateCall)
            .anyMatch(f -> f != null && f.getIntermediateFields() != null
                && f.getIntermediateFields().stream().anyMatch(
                    iField -> iField.getFieldType().getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary
                ));
        if (!needsOverride) return ArrowSchemaFromCalcite.arrowSchemaFromRowType(rowType);

        List<Field> fields = new ArrayList<>();
        int groupCount = agg.getGroupSet().cardinality();
        for (int i = 0; i < groupCount; i++) {
            fields.add(ArrowSchemaFromCalcite.fieldFromCalcite(rowType.getFieldList().get(i)));
        }
        int colIdx = groupCount;
        for (var call : agg.getAggCallList()) {
            var func = AggregateFunction.fromAggregateCall(call);
            var iFields = func != null ? func.getIntermediateFields() : null;
            var f = rowType.getFieldList().get(colIdx++);
            boolean hasBinary = iFields != null && iFields.stream()
                .anyMatch(iField -> iField.getFieldType().getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary);
            if (hasBinary) {
                // DC: use Binary to match DataFusion's partial HLL sketch output
                for (var iField : iFields) {
                    String name = iField.getName().isEmpty() ? f.getName() : f.getName() + iField.getName();
                    fields.add(new Field(name, iField.getFieldType(), null));
                }
            } else {
                fields.add(ArrowSchemaFromCalcite.fieldFromCalcite(f));
            }
        }
        return new Schema(fields);
    }

    private static org.apache.calcite.rel.core.Aggregate findAggregate(org.apache.calcite.rel.RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.Aggregate agg) return agg;
        if (node.getInputs().size() == 1) return findAggregate(node.getInputs().get(0));
        return null;
    }
}

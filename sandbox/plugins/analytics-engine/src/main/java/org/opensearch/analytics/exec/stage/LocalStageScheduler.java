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
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.CapabilityRegistry;
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
 * Builds executions for {@link StageExecutionType#COORDINATOR_REDUCE} stages —
 * those that run at the coordinator with a backend-provided {@link ExchangeSink}.
 * Creates the sink via {@link Stage#getExchangeSinkProvider()} using an
 * {@link ExchangeSinkContext} carrying the plan bytes, allocator, per-child
 * input descriptors (one per child stage, each with its stage id + Arrow
 * schema), and the downstream sink. Hands the resulting sink to
 * {@link LocalStageExecution}.
 *
 * <p>Multi-child stages (Union, future Join) are routed via
 * {@link LocalStageExecution#inputSink(int)}, which returns a per-child
 * wrapper that the backend sink uses to register a distinct input partition
 * per child stage id.
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
            buildChildInputs(stage),
            sink
        );

        // Apply instruction handlers for the reduce stage.
        // Unlike AnalyticsSearchService (shard path) which resolves the factory from its
        // local backends map, the coordinator-reduce path has no backends map — the factory
        // is stored on the Stage during FragmentConversionDriver.convertAll (root stage only,
        // no serialization needed since reduce executes locally at the coordinator).
        // TODO: find a cleaner way to provide the factory without storing it on Stage.
        FragmentInstructionHandlerFactory factory = stage.getInstructionHandlerFactory();
        if (factory != null) {
            BackendExecutionContext backendContext = null;
            for (InstructionNode node : stage.getPlanAlternatives().getFirst().instructions()) {
                FragmentInstructionHandler handler = factory.createHandler(node);
                backendContext = handler.apply(node, context, backendContext);
            }
        }

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
     * Builds one {@link ExchangeSinkContext.ChildInput} per child stage. Each entry
     * carries the child's stage id (used by the backend to namespace its registered
     * input, e.g. {@code "input-<stageId>"}) and the Arrow schema derived from the
     * child fragment's row type.
     */
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

    private Schema deriveChildSchema(Stage child) {
        RelNode childFragment = child.getPlanAlternatives().isEmpty()
            ? child.getFragment()
            : child.getPlanAlternatives().getFirst().resolvedFragment();

        Aggregate agg = findAggregate(childFragment);
        if (agg == null) {
            return ArrowSchemaFromCalcite.arrowSchemaFromRowType(childFragment.getRowType());
        }

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
            List<Field> intermediateFields = resolveIntermediateFields(call, backendId);
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

    private List<Field> resolveIntermediateFields(AggregateCall call, String backendId) {
        if (backendId == null || capabilityRegistry == null) return null;
        AggregateFunction func = AggregateFunction.fromAggregateCall(call);
        if (func == null) return null;
        return capabilityRegistry.getIntermediateFields(backendId, func);
    }

    private static Aggregate findAggregate(RelNode node) {
        if (node instanceof Aggregate agg) return agg;
        if (node.getInputs().size() == 1) return findAggregate(node.getInputs().get(0));
        return null;
    }
}

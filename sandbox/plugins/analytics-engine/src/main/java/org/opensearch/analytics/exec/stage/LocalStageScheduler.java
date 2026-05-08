/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
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
 * The streaming table schema is derived directly from the child fragment's row type,
 * which is correct because {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule}
 * decomposes aggregates into their partial/final equivalents at planning time.
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

    private Schema deriveChildSchema(Stage child) {
        var childFragment = child.getPlanAlternatives().isEmpty()
            ? child.getFragment()
            : child.getPlanAlternatives().getFirst().resolvedFragment();
        return ArrowSchemaFromCalcite.arrowSchemaFromRowType(childFragment.getRowType());
    }
}

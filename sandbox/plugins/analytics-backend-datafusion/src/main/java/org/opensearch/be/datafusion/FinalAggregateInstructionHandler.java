/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.FinalAggregateInstructionNode;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handles FinalAggregate instruction for coordinator-reduce stages.
 * Creates a LocalSession, registers streaming partitions, and prepares
 * the physical plan in final aggregate mode.
 */
class FinalAggregateInstructionHandler implements FragmentInstructionHandler<FinalAggregateInstructionNode> {

    private final DataFusionPlugin plugin;

    FinalAggregateInstructionHandler(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        FinalAggregateInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ExchangeSinkContext ctx = (ExchangeSinkContext) commonContext;
        NativeRuntimeHandle runtime = plugin.getDataFusionService().getNativeRuntime();
        DatafusionLocalSession session = new DatafusionLocalSession(runtime.get());

        // Register streaming partitions using schema derived from child fragment
        Map<Integer, DatafusionPartitionSender> senders = new LinkedHashMap<>(ctx.childInputs().size());
        for (ExchangeSinkContext.ChildInput child : ctx.childInputs()) {
            byte[] schemaIpc = ArrowSchemaIpc.toBytes(child.schema());
            String inputId = "input-" + child.childStageId();
            long senderPtr = NativeBridge.registerPartitionStream(session.getPointer(), inputId, schemaIpc);
            senders.put(child.childStageId(), new DatafusionPartitionSender(senderPtr));
        }

        // Prepare the final-aggregate plan (streaming tables are registered, plan resolves)
        NativeBridge.prepareFinalPlan(session.getPointer(), ctx.fragmentBytes());

        return new DataFusionReduceState(session, runtime, senders);
    }
}

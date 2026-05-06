/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.PartialAggregateInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

/**
 * Handles PartialAggregate instruction: prepares the physical plan in partial
 * aggregate mode on the native SessionContext. The prepared plan is stored on
 * the handle and executed later by the searcher.
 */
class PartialAggregateInstructionHandler implements FragmentInstructionHandler<PartialAggregateInstructionNode> {

    @Override
    public BackendExecutionContext apply(
        PartialAggregateInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        DataFusionSessionState state = (DataFusionSessionState) backendContext;
        ShardScanExecutionContext ctx = (ShardScanExecutionContext) commonContext;
        NativeBridge.preparePartialPlan(state.sessionContextHandle().getPointer(), ctx.getFragmentBytes());
        return state;
    }
}

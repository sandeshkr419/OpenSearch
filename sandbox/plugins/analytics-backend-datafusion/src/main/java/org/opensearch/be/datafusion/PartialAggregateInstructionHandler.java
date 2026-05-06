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
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.PartialAggregateInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

/**
 * Handles PartialAggregate instruction: configures the native SessionContext
 * for partial aggregate mode so the Rust executor emits intermediate state.
 */
class PartialAggregateInstructionHandler implements FragmentInstructionHandler<PartialAggregateInstructionNode> {

    @Override
    public BackendExecutionContext apply(
        PartialAggregateInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        DataFusionSessionState state = (DataFusionSessionState) backendContext;
        NativeBridge.setPartialAggregateMode(state.sessionContextHandle().getPointer());
        return state;
    }
}

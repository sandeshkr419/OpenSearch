/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.AggregateExecutionMode;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.PartialAggregateInstructionNode;

/**
 * Handles PartialAggregate instruction: sets mode=PARTIAL on the session state
 * so the Rust executor emits intermediate aggregate state instead of final results.
 */
public class PartialAggregateInstructionHandler implements FragmentInstructionHandler<PartialAggregateInstructionNode> {

    @Override
    public BackendExecutionContext apply(
        PartialAggregateInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        DataFusionSessionState prev = (DataFusionSessionState) backendContext;
        return new DataFusionSessionState(prev.sessionContextHandle(), AggregateExecutionMode.PARTIAL);
    }
}

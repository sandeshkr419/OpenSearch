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
import org.opensearch.analytics.spi.FinalAggregateInstructionNode;
import org.opensearch.analytics.spi.FragmentInstructionHandler;

/**
 * Handles FinalAggregate instruction for coordinator-reduce stages.
 * The coordinator's LocalSession is configured for final mode via executeLocalPlan.
 */
class FinalAggregateInstructionHandler implements FragmentInstructionHandler<FinalAggregateInstructionNode> {

    @Override
    public BackendExecutionContext apply(
        FinalAggregateInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        // Coordinator-reduce path: final mode is inherent to the reduce sink (executeLocalPlanFinal).
        // No session context to configure here — the LocalSession is created by the sink.
        return backendContext;
    }
}

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
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;

/**
 * Backend-specific execution context produced by instruction handlers,
 * consumed by DatafusionSearcher at execute time.
 *
 * @param sessionContextHandle native session context (null for coordinator-reduce path)
 * @param mode aggregate execution mode
 */
public record DataFusionSessionState(SessionContextHandle sessionContextHandle, AggregateExecutionMode mode)
    implements
        BackendExecutionContext {

    /** Default mode constructor for backward compatibility. */
    public DataFusionSessionState(SessionContextHandle sessionContextHandle) {
        this(sessionContextHandle, AggregateExecutionMode.DEFAULT);
    }
}

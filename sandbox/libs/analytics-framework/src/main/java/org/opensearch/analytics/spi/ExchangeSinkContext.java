/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Context passed to {@link ExchangeSinkProvider#createSink} when a
 * coordinator-reduce stage is being set up.
 *
 * @opensearch.internal
 */
public record ExchangeSinkContext(String queryId, int stageId, byte[] fragmentBytes, BufferAllocator allocator, List<
    ChildInput> childInputs, ExchangeSink downstream) implements CommonExecutionContext {

    /** Per-child input descriptor: the child stage id and the Arrow schema of its outgoing batches. */
    public record ChildInput(int childStageId, Schema schema) {
    }

    /**
     * Convenience for single-input back-compat. Returns the schema of the sole child input.
     */
    public Schema inputSchema() {
        if (childInputs.size() != 1) {
            throw new IllegalStateException(
                "inputSchema() requires exactly one child input; got " + childInputs.size() + " — use childInputs() instead"
            );
        }
        return childInputs.get(0).schema();
    }
}

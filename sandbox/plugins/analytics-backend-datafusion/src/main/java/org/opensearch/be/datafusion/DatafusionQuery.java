/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.AggregateExecutionMode;

/**
 * Represents a DataFusion query — wraps substrait plan bytes and execution metadata.
 */
public class DatafusionQuery {

    private final String indexName;
    private final byte[] substraitBytes;
    private boolean fetchPhase;
    private long contextId;
    private final AggregateExecutionMode mode;

    public DatafusionQuery(String indexName, byte[] substraitBytes, long contextId, AggregateExecutionMode mode) {
        this.indexName = indexName;
        this.substraitBytes = substraitBytes;
        this.contextId = contextId;
        this.mode = mode;
    }

    public DatafusionQuery(String indexName, byte[] substraitBytes, long contextId) {
        this(indexName, substraitBytes, contextId, AggregateExecutionMode.DEFAULT);
    }

    /** Returns the target index name. */
    public String getIndexName() {
        return indexName;
    }

    /** Returns the serialized substrait plan bytes. */
    public byte[] getSubstraitBytes() {
        return substraitBytes;
    }

    /** Returns whether this query is in the fetch phase. */
    public boolean isFetchPhase() {
        return fetchPhase;
    }

    /**
     * Sets whether this query is in the fetch phase.
     * @param fetchPhase true if this query is in the fetch phase
     */
    public void setFetchPhase(boolean fetchPhase) {
        this.fetchPhase = fetchPhase;
    }

    /** Returns the query context ID for per-query memory tracking. */
    public long getContextId() {
        return contextId;
    }

    /** Returns the aggregation execution mode. */
    public AggregateExecutionMode getMode() {
        return mode;
    }
}

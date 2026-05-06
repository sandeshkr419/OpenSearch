/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

/**
 * Aggregation execution mode passed from instruction handlers to the backend runtime.
 * Used by NativeBridge to dispatch to the appropriate native execution function.
 */
public enum AggregateExecutionMode {
    /** No mode forcing — plain scan or full aggregation. */
    DEFAULT,
    /** Shard emits intermediate aggregate state (e.g. HLL sketch bytes, partial SUM+COUNT). */
    PARTIAL,
    /** Coordinator merges partial state from shards into the final result. */
    FINAL
}

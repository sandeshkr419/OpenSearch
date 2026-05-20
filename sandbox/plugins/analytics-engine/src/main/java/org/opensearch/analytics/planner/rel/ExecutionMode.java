/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

/**
 * Execution mode for distributed query operators that may be split across
 * shards and the coordinator. Used by {@link OpenSearchAggregate} and
 * {@link OpenSearchSort}.
 *
 * @opensearch.internal
 */
public enum ExecutionMode {
    /** Partial computation at data nodes (emits intermediate state for later merge). */
    PARTIAL,
    /** Final computation at coordinator (merges partial state from data nodes). */
    FINAL,
    /** No split needed — full computation in one pass on a single node. */
    SINGLE
}

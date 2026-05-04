/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

/**
 * Aggregation execution mode passed from Core to the backend alongside a plan fragment.
 * The ordinal maps directly to the FFI contract with the Rust backend:
 * 0 = DEFAULT, 1 = PARTIAL, 2 = FINAL.
 */
public enum AggregateExecutionMode {
    /** No mode forcing — plain scan or full aggregation. */
    DEFAULT(0),
    /** Shard emits intermediate aggregate state (e.g. HLL sketch bytes, partial SUM+COUNT). */
    PARTIAL(1),
    /** Coordinator merges partial state from shards into the final result. */
    FINAL(2);

    private final int value;

    AggregateExecutionMode(int value) {
        this.value = value;
    }

    /** Returns the FFI value for this mode (matches the Rust-side constant). */
    public int value() {
        return value;
    }
}

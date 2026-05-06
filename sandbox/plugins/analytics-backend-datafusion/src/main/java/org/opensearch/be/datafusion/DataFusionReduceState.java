/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendExecutionContext;

import java.util.Map;

/**
 * Backend execution context for coordinator-reduce stages.
 * Carries a pre-configured LocalSession with registered streaming partitions
 * and a prepared plan ready for execution.
 */
record DataFusionReduceState(DatafusionLocalSession session, NativeRuntimeHandle runtimeHandle, Map<
    Integer,
    DatafusionPartitionSender> senders) implements BackendExecutionContext {
}

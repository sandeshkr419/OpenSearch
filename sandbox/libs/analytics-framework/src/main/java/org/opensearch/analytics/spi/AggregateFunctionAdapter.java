/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.core.AggregateCall;

/**
 * Rewrites an {@link AggregateCall} before Substrait serialization. Backends register
 * adapters to map framework-level aggregate calls to backend-specific operators.
 *
 * @opensearch.internal
 */
public interface AggregateFunctionAdapter {

    /** Returns the rewritten call, or the original if no adaptation is needed. */
    AggregateCall adapt(AggregateCall call);
}

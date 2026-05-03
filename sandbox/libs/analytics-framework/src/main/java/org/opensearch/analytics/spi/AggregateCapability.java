/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.common.Nullable;

import java.util.Set;

/**
 * Declares that a backend can evaluate a specific {@link AggregateFunction}
 * on a specific {@link FieldType} in the given data formats.
 *
 * <p>Flat record because all subcategories share the same shape. The category
 * lives on {@link AggregateFunction#getType()}. Per-type factory methods
 * validate the function type at construction and make backend declarations
 * self-documenting.
 *
 * <p>{@link #decomposition()} is null for most functions — the planner applies
 * Calcite's standard decomposition (AVG → SUM/COUNT, STDDEV → SUM(x²)+SUM(x)+COUNT).
 * Backends with non-standard partial state (e.g. HLL sketches, Welford STDDEV)
 * provide a custom {@link AggregateDecomposition}.
 *
 * <p>TODO (plan forking): during resolution of a plan alternative, after a single
 * backend is chosen for an aggregate operator, apply decomposition as a paired
 * rewrite of PARTIAL output schema + FINAL input schema:
 * <ol>
 *   <li>If decomposition == null: apply Calcite's AggregateReduceFunctionsRule
 *       to the PARTIAL+FINAL pair.</li>
 *   <li>If decomposition != null: use decomposition.partialCalls() to rewrite
 *       PARTIAL's aggCalls and output row type, then use decomposition.finalExpression()
 *       to rewrite FINAL's aggCalls. Both must be updated together — the exchange
 *       row type between them must be consistent.</li>
 * </ol>
 *
 * @opensearch.internal
 */
public record AggregateCapability(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats,
    @Nullable AggregateDecomposition decomposition,
    @Nullable ArrowType intermediateArrowType) {

    /** Convenience constructor with no custom decomposition and no intermediate type override. */
    public AggregateCapability(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        this(function, fieldTypes, formats, null, null);
    }

    /** Convenience constructor with decomposition but no intermediate type override. */
    public AggregateCapability(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats,
        @Nullable AggregateDecomposition decomposition) {
        this(function, fieldTypes, formats, decomposition, null);
    }

    public static AggregateCapability simple(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.SIMPLE;
        return new AggregateCapability(function, fieldTypes, formats);
    }

    public static AggregateCapability statistical(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.STATISTICAL;
        return new AggregateCapability(function, fieldTypes, formats);
    }

    public static AggregateCapability stateExpanding(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.STATE_EXPANDING;
        return new AggregateCapability(function, fieldTypes, formats);
    }

    /**
     * Factory for approximate functions that emit non-standard intermediate state.
     * {@code intermediateArrowType} declares the Arrow type of the partial output
     * (e.g. {@code Binary} for HLL sketch bytes), used by the coordinator to set
     * the correct streaming table schema.
     */
    public static AggregateCapability approximate(AggregateFunction function, Set<FieldType> fieldTypes,
        Set<String> formats, ArrowType intermediateArrowType) {
        assert function.getType() == AggregateFunction.Type.APPROXIMATE;
        return new AggregateCapability(function, fieldTypes, formats, null, intermediateArrowType);
    }

    public static AggregateCapability approximate(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        assert function.getType() == AggregateFunction.Type.APPROXIMATE;
        return new AggregateCapability(function, fieldTypes, formats);
    }
}

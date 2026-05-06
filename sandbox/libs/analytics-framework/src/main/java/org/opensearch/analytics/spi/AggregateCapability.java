/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.common.Nullable;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Declares that a backend can evaluate a specific {@link AggregateFunction}
 * on a specific {@link FieldType} in the given data formats.
 *
 * <p>{@link #intermediateFields()} is non-null when the backend's partial state differs
 * from the Calcite-declared return type. Each entry describes one column of the partial
 * state (name suffix + Arrow type). The framework uses this to build the streaming table
 * schema and expand the coordinator's scan row type.
 *
 * <p>{@link #finalExpression()} is non-null when the coordinator must combine the partial
 * state columns into the final result using a custom expression (e.g. AVG = sum/count).
 *
 * @opensearch.internal
 */
public record AggregateCapability(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats,
    @Nullable AggregateDecomposition decomposition, @Nullable List<Field> intermediateFields, @Nullable BiFunction<
        RexBuilder,
        List<RexNode>,
        RexNode> finalExpression) {

    public AggregateCapability(AggregateFunction function, Set<FieldType> fieldTypes, Set<String> formats) {
        this(function, fieldTypes, formats, null, null, null);
    }

    public AggregateCapability(
        AggregateFunction function,
        Set<FieldType> fieldTypes,
        Set<String> formats,
        @Nullable AggregateDecomposition decomposition
    ) {
        this(function, fieldTypes, formats, decomposition, null, null);
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

    /** Factory for functions whose partial state is a single non-standard Arrow type. */
    public static AggregateCapability approximate(
        AggregateFunction function,
        Set<FieldType> fieldTypes,
        Set<String> formats,
        ArrowType intermediateArrowType
    ) {
        assert function.getType() == AggregateFunction.Type.APPROXIMATE;
        Field field = new Field("", new org.apache.arrow.vector.types.pojo.FieldType(true, intermediateArrowType, null), null);
        return new AggregateCapability(function, fieldTypes, formats, null, List.of(field), null);
    }

    /**
     * Factory for functions whose partial state spans multiple fields and whose final
     * result requires a custom combination expression over those fields.
     */
    public static AggregateCapability withIntermediateFields(
        AggregateFunction function,
        Set<FieldType> fieldTypes,
        Set<String> formats,
        List<Field> intermediateFields,
        BiFunction<RexBuilder, List<RexNode>, RexNode> finalExpression
    ) {
        return new AggregateCapability(function, fieldTypes, formats, null, intermediateFields, finalExpression);
    }
}

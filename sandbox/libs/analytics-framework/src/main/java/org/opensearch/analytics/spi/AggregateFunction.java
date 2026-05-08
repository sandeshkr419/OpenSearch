/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Aggregate functions that a backend may support, categorized by {@link Type}.
 *
 * <p>Note: {@code COUNT} covers both {@code COUNT(*)} and {@code COUNT(DISTINCT x)}.
 * The distinction is on {@code AggregateCall.isDistinct()}, not on SqlKind.
 *
 * @opensearch.internal
 */
public enum AggregateFunction {
    // Simple — fixed-size state per key
    SUM(Type.SIMPLE, SqlKind.SUM),
    SUM0(Type.SIMPLE, SqlKind.SUM0),
    MIN(Type.SIMPLE, SqlKind.MIN),
    MAX(Type.SIMPLE, SqlKind.MAX),
    COUNT(
        Type.SIMPLE,
        SqlKind.COUNT,
        List.of(new Field("", new FieldType(false, new ArrowType.Int(64, true), null), null)),
        (rb, refs) -> refs.get(0)
    ),
    AVG(
        Type.SIMPLE,
        SqlKind.AVG,
        List.of(
            new Field("[count]", new FieldType(false, new ArrowType.Int(64, true), null), null),
            new Field("[sum]", new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), null)
        ),
        (rb, refs) -> {
            RelDataType dbl = rb.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
            return rb.makeCall(SqlStdOperatorTable.DIVIDE, rb.makeCast(dbl, refs.get(1)), rb.makeCast(dbl, refs.get(0)));
        }
    ),

    // Statistical — fixed-size state, multi-pass or running stats
    STDDEV_POP(Type.STATISTICAL, SqlKind.STDDEV_POP),
    STDDEV_SAMP(Type.STATISTICAL, SqlKind.STDDEV_SAMP),
    VAR_POP(Type.STATISTICAL, SqlKind.VAR_POP),
    VAR_SAMP(Type.STATISTICAL, SqlKind.VAR_SAMP),

    // State-expanding — state grows with input rows per key
    PERCENTILE_CONT(Type.STATE_EXPANDING, SqlKind.PERCENTILE_CONT),
    PERCENTILE_DISC(Type.STATE_EXPANDING, SqlKind.PERCENTILE_DISC),
    COLLECT(Type.STATE_EXPANDING, SqlKind.COLLECT),
    LISTAGG(Type.STATE_EXPANDING, SqlKind.LISTAGG),

    // Approximate — probabilistic, fixed-size state
    APPROX_COUNT_DISTINCT(
        Type.APPROXIMATE,
        SqlKind.OTHER,
        List.of(new Field("", new FieldType(false, ArrowType.Binary.INSTANCE, null), null)),
        null
    );

    /** Category of aggregate function. Affects execution strategy (shuffle vs map-reduce). */
    public enum Type {
        SIMPLE,
        STATISTICAL,
        STATE_EXPANDING,
        APPROXIMATE
    }

    private final Type type;
    private final SqlKind sqlKind;
    private final List<Field> intermediateFields;
    private final BiFunction<RexBuilder, List<RexNode>, RexNode> finalExpression;

    AggregateFunction(Type type, SqlKind sqlKind) {
        this(type, sqlKind, null, null);
    }

    AggregateFunction(
        Type type,
        SqlKind sqlKind,
        List<Field> intermediateFields,
        BiFunction<RexBuilder, List<RexNode>, RexNode> finalExpression
    ) {
        this.type = type;
        this.sqlKind = sqlKind;
        this.intermediateFields = intermediateFields;
        this.finalExpression = finalExpression;
    }

    public Type getType() {
        return type;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Returns the Arrow fields for partial aggregate state, or null if no intermediate expansion needed. */
    public List<Field> getIntermediateFields() {
        return intermediateFields;
    }

    /** Returns true if any intermediate field has Binary type (e.g. DC emits an HLL sketch). */
    public boolean hasBinaryIntermediateField() {
        return intermediateFields != null
            && intermediateFields.stream()
                .anyMatch(f -> f.getFieldType().getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Binary);
    }

    /** Returns the expression to compute the final result from intermediate columns, or null. */
    public BiFunction<RexBuilder, List<RexNode>, RexNode> getFinalExpression() {
        return finalExpression;
    }

    /** Maps a Calcite SqlKind to an AggregateFunction, or null if not recognized. Skips OTHER. */
    public static AggregateFunction fromSqlKind(SqlKind kind) {
        for (AggregateFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER) {
                return func;
            }
        }
        return null;
    }

    /** Backend-specific operator names that map to a known AggregateFunction. */
    private static final java.util.Map<String, AggregateFunction> NAME_ALIASES = java.util.Map.of("approx_distinct", APPROX_COUNT_DISTINCT);

    /**
     * Resolves the {@link AggregateFunction} for a Calcite {@link org.apache.calcite.rel.core.AggregateCall}.
     * Checks SqlKind first, then falls back to name matching (enum name or registered alias).
     */
    public static AggregateFunction fromAggregateCall(org.apache.calcite.rel.core.AggregateCall call) {
        AggregateFunction func = fromSqlKind(call.getAggregation().getKind());
        if (func == null) {
            String name = call.getAggregation().getName();
            func = NAME_ALIASES.get(name.toLowerCase(java.util.Locale.ROOT));
            if (func == null) {
                try {
                    func = fromNameOrError(name);
                } catch (IllegalStateException ignored) {}
            }
        }
        return func;
    }

    /** Maps an aggregate function name to an AggregateFunction. Throws if not recognized. */
    public static AggregateFunction fromNameOrError(String name) {
        try {
            return valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized aggregate function [" + name + "]", e);
        }
    }
}

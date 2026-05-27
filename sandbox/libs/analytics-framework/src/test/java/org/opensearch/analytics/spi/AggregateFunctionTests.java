/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.opensearch.analytics.spi.AggregateFunction.APPROX_COUNT_DISTINCT;
import static org.opensearch.analytics.spi.AggregateFunction.AVG;
import static org.opensearch.analytics.spi.AggregateFunction.COUNT;
import static org.opensearch.analytics.spi.AggregateFunction.FIRST;
import static org.opensearch.analytics.spi.AggregateFunction.LAST;
import static org.opensearch.analytics.spi.AggregateFunction.LIST;
import static org.opensearch.analytics.spi.AggregateFunction.MAX;
import static org.opensearch.analytics.spi.AggregateFunction.MIN;
import static org.opensearch.analytics.spi.AggregateFunction.SUM;
import static org.opensearch.analytics.spi.AggregateFunction.TAKE;
import static org.opensearch.analytics.spi.AggregateFunction.VALUES;

/**
 * Asserts the enum carries the right shape per function for the resolver's three
 * single-field decomposition cases: pass-through (no intermediate), function-swap
 * (reducer ≠ self), engine-native merge (reducer == self, binary intermediate).
 *
 * <p>Multi-field / scalar-final shapes (AVG, STDDEV, VAR) are <em>not</em> encoded on
 * the enum — they're handled by {@code OpenSearchAggregateReduceRule} during HEP
 * marking using Calcite's {@code AggregateReduceFunctionsRule}. The enum entries for
 * those functions intentionally declare {@code intermediateFields == null} so that
 * the resolver's pass-through branch catches any post-reduction primitive calls.
 */
public class AggregateFunctionTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);

    private RelDataType resolve(AggregateFunction.IntermediateField field, RelDataType arg0) {
        return field.typeResolver().resolve(List.of(arg0), typeFactory);
    }

    // ── Pass-through: SUM / MIN / MAX ──
    public void testSumHasDecomposition() {
        assertTrue(SUM.hasDecomposition());
        assertTrue(SUM.isEngineNativeMerge());
    }
    // ── COUNT: engine-native merge (Binary state, self-reducer) ──

    public void testCountHasDecomposition() {
        assertTrue(COUNT.hasDecomposition());
        assertTrue(COUNT.isEngineNativeMerge());
    }

    public void testCountIntermediateFields() {
        List<AggregateFunction.IntermediateField> fields = COUNT.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("count_state", fields.get(0).name());
        assertSame(COUNT, fields.get(0).reducer());
    }

    // ── AVG / STDDEV / VAR: state-shipping (single Binary intermediate, reducer == self) ──

    public void testAvgHasDecomposition() {
        // AVG ships state on the wire as a single Binary column; engine-native merge
        // (FINAL aggregator = AVG, reducer == self).
        assertTrue(AVG.hasDecomposition());
        List<AggregateFunction.IntermediateField> fields = AVG.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("avg_state", fields.get(0).name());
        assertSame(AVG, fields.get(0).reducer());
    }

    // ── APPROX_COUNT_DISTINCT: engine-native (single binary field, reducer == self) ──

    public void testApproxCountDistinctHasDecomposition() {
        assertTrue(APPROX_COUNT_DISTINCT.hasDecomposition());
    }

    public void testApproxCountDistinctReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = APPROX_COUNT_DISTINCT.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("sketch", fields.get(0).name());
        assertSame(APPROX_COUNT_DISTINCT, fields.get(0).reducer());
        assertEquals(SqlTypeName.VARBINARY, resolve(fields.get(0), integer).getSqlTypeName());
    }

    // ── TAKE: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testTakeHasDecomposition() {
        assertTrue(TAKE.hasDecomposition());
    }

    public void testTakeReducerIsSelfAndResolverIsParameterised() {
        List<AggregateFunction.IntermediateField> fields = TAKE.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("take_state", fields.get(0).name());
        assertSame(TAKE, fields.get(0).reducer());
        assertEquals("passThroughArg0 returns arg0", integer, resolve(fields.get(0), integer));
    }

    // ── FIRST: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testFirstHasDecomposition() {
        assertTrue(FIRST.hasDecomposition());
    }

    public void testFirstReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = FIRST.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("first_state", fields.get(0).name());
        assertSame(FIRST, fields.get(0).reducer());
        assertEquals(integer, resolve(fields.get(0), integer));
    }

    // ── LAST: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testLastHasDecomposition() {
        assertTrue(LAST.hasDecomposition());
    }

    public void testLastReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = LAST.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("last_state", fields.get(0).name());
        assertSame(LAST, fields.get(0).reducer());
        assertEquals(integer, resolve(fields.get(0), integer));
    }

    // ── LIST: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testListHasDecomposition() {
        assertTrue(LIST.hasDecomposition());
    }

    public void testListReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = LIST.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("list_state", fields.get(0).name());
        assertSame(LIST, fields.get(0).reducer());
        assertEquals(integer, resolve(fields.get(0), integer));
    }

    // ── VALUES: engine-native (single field, reducer == self, parameterised resolver) ──

    public void testValuesHasDecomposition() {
        assertTrue(VALUES.hasDecomposition());
    }

    public void testValuesReducerIsSelf() {
        List<AggregateFunction.IntermediateField> fields = VALUES.intermediateFields();
        assertEquals(1, fields.size());
        assertEquals("values_state", fields.get(0).name());
        assertSame(VALUES, fields.get(0).reducer());
        assertEquals(integer, resolve(fields.get(0), integer));
    }

    // ── fromSqlKind still works ──

    public void testFromSqlKindResolvesExistingEntries() {
        assertSame(SUM, AggregateFunction.fromSqlKind(SqlKind.SUM));
        assertSame(MIN, AggregateFunction.fromSqlKind(SqlKind.MIN));
        assertSame(MAX, AggregateFunction.fromSqlKind(SqlKind.MAX));
        assertSame(COUNT, AggregateFunction.fromSqlKind(SqlKind.COUNT));
        assertSame(AVG, AggregateFunction.fromSqlKind(SqlKind.AVG));
    }

    public void testFromSqlKindReturnsNullForOther() {
        assertNull(AggregateFunction.fromSqlKind(SqlKind.OTHER));
    }
}

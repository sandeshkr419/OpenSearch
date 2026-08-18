/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link NumericToDoubleAdapter}, the widening adapter used by {@code cbrt},
 * {@code cot}, {@code exp}, {@code log10}, {@code sin}, {@code cos}, {@code tan} and
 * {@code degrees}.
 *
 * <p>Contract: every numeric operand narrower than {@code DOUBLE} is widened to {@code DOUBLE},
 * {@code DOUBLE} passes through, non-numeric operands are untouched, and the call's declared return
 * type is preserved.
 */
public class NumericToDoubleAdapterTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    private RelDataType type(SqlTypeName name, boolean nullable) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(name), nullable);
    }

    private RexCall cbrtCall(RexNode operand) {
        return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CBRT, List.of(operand));
    }

    /** Asserts the adapted call's operand is DOUBLE. */
    private void assertOperandWidened(RexNode operand, String label) {
        RexCall adapted = (RexCall) new NumericToDoubleAdapter(SqlStdOperatorTable.CBRT).adapt(cbrtCall(operand), List.of(), cluster);
        assertSame("operator preserved for " + label, SqlStdOperatorTable.CBRT, adapted.getOperator());
        assertEquals(
            label + " operand must be widened to DOUBLE",
            SqlTypeName.DOUBLE,
            adapted.getOperands().get(0).getType().getSqlTypeName()
        );
    }

    // ── Every narrower numeric width must widen ─────────────────────────────

    public void testTinyintOperandIsWidened() {
        assertOperandWidened(rexBuilder.makeLiteral(5, type(SqlTypeName.TINYINT, false), false), "TINYINT");
    }

    public void testSmallintOperandIsWidened() {
        assertOperandWidened(rexBuilder.makeLiteral(5, type(SqlTypeName.SMALLINT, false), false), "SMALLINT");
    }

    public void testIntegerOperandIsWidened() {
        assertOperandWidened(rexBuilder.makeLiteral(5, type(SqlTypeName.INTEGER, false), false), "INTEGER");
    }

    public void testBigintOperandIsWidened() {
        assertOperandWidened(rexBuilder.makeLiteral(5L, type(SqlTypeName.BIGINT, false), false), "BIGINT");
    }

    public void testFloatOperandIsWidened() {
        assertOperandWidened(rexBuilder.makeLiteral(BigDecimal.valueOf(5.5), type(SqlTypeName.FLOAT, false), false), "FLOAT");
    }

    public void testRealOperandIsWidened() {
        assertOperandWidened(rexBuilder.makeLiteral(BigDecimal.valueOf(5.5), type(SqlTypeName.REAL, false), false), "REAL");
    }

    public void testDecimalOperandIsWidened() {
        RelDataType decimalType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2), false);
        assertOperandWidened(rexBuilder.makeLiteral(BigDecimal.valueOf(123, 2), decimalType, false), "DECIMAL");
    }

    // ── Pass-through cases ──────────────────────────────────────────────────

    public void testDoubleOperandIsNotRewrapped() {
        RexNode dbl = rexBuilder.makeLiteral(BigDecimal.valueOf(5.5), type(SqlTypeName.DOUBLE, false), false);
        RexCall adapted = (RexCall) new NumericToDoubleAdapter(SqlStdOperatorTable.CBRT).adapt(cbrtCall(dbl), List.of(), cluster);
        assertSame("DOUBLE operand must pass through by reference, not be re-cast", dbl, adapted.getOperands().get(0));
    }

    /** A non-numeric operand passes through unchanged. */
    public void testNonNumericOperandIsUntouched() {
        RexNode varchar = rexBuilder.makeLiteral("abc", type(SqlTypeName.VARCHAR, false), false);
        RexNode widened = NumericToDoubleAdapter.widenToDoubleIfNumeric(varchar, cluster);
        assertSame("VARCHAR operand must pass through unchanged", varchar, widened);
    }

    // ── Type-preservation invariants ────────────────────────────────────────

    /** Widening an operand must not change the call's declared return type. */
    public void testOuterReturnTypeIsPreserved() {
        RexCall original = cbrtCall(rexBuilder.makeLiteral(5, type(SqlTypeName.INTEGER, false), false));
        RexNode adapted = new NumericToDoubleAdapter(SqlStdOperatorTable.CBRT).adapt(original, List.of(), cluster);
        assertEquals("outer return type must match the original call", original.getType(), adapted.getType());
    }

    public void testNullableOperandKeepsNullabilityWhenWidened() {
        RexNode nullableInt = rexBuilder.makeNullLiteral(type(SqlTypeName.INTEGER, true));
        RexNode widened = NumericToDoubleAdapter.widenToDoubleIfNumeric(nullableInt, cluster);
        assertEquals("widened to DOUBLE", SqlTypeName.DOUBLE, widened.getType().getSqlTypeName());
        assertTrue("nullability preserved through the widening cast", widened.getType().isNullable());
    }

    public void testNonNullableOperandStaysNonNullable() {
        RexNode operand = rexBuilder.makeLiteral(5, type(SqlTypeName.INTEGER, false), false);
        RexNode widened = NumericToDoubleAdapter.widenToDoubleIfNumeric(operand, cluster);
        assertFalse("non-nullability preserved through the widening cast", widened.getType().isNullable());
    }

    /** Re-adapting an already-widened call must not nest another cast. */
    public void testReAdaptingAlreadyWidenedCallIsStable() {
        NumericToDoubleAdapter adapter = new NumericToDoubleAdapter(SqlStdOperatorTable.CBRT);
        RexCall once = (RexCall) adapter.adapt(
            cbrtCall(rexBuilder.makeLiteral(5, type(SqlTypeName.INTEGER, false), false)),
            List.of(),
            cluster
        );
        RexCall twice = (RexCall) adapter.adapt(once, List.of(), cluster);
        assertSame("second pass must not add another cast layer", once.getOperands().get(0), twice.getOperands().get(0));
    }
}

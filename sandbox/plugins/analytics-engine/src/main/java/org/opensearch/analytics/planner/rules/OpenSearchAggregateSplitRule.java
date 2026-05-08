/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits an {@link OpenSearchAggregate}(SINGLE) into PARTIAL + Exchange + FINAL.
 *
 * <p>PARTIAL aggCalls are decomposed for multi-field intermediate state:
 * AVG(x) → COUNT(x) + SUM(x), so the Calcite row type matches DataFusion's partial output.
 * All other functions keep their original call for partial.
 *
 * <p>FINAL aggCalls keep the original calls adapted to reference the partial output columns.
 * DataFusion handles the partial→final merge internally for each aggregate function.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateSplitRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchAggregateSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateSplitRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        return aggregate.getMode() == AggregateMode.SINGLE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);
        int groupCount = aggregate.getGroupSet().cardinality();
        RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

        // Build partial aggCalls. For AVG (multi-field intermediate), expand to COUNT + SUM
        // so the Calcite row type matches DataFusion's actual partial output schema.
        List<AggregateCall> partialCalls = new ArrayList<>();
        int[] partialStart = new int[aggregate.getAggCallList().size()];

        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            AggregateCall origCall = aggregate.getAggCallList().get(i);
            AggregateFunction func = AggregateFunction.fromAggregateCall(origCall);
            List<org.apache.arrow.vector.types.pojo.Field> iFields = func != null ? func.getIntermediateFields() : null;
            partialStart[i] = partialCalls.size();

            if (iFields != null && iFields.size() > 1) {
                // Multi-field intermediate state (e.g. AVG → COUNT + SUM).
                // Use the types from intermediateFields to match DataFusion's partial output.
                for (int j = 0; j < iFields.size(); j++) {
                    var iField = iFields.get(j);
                    var colType = arrowToCalcite(iField.getFieldType().getType(), typeFactory);
                    var aggFn = j == 0 ? SqlStdOperatorTable.COUNT : SqlStdOperatorTable.SUM;
                    partialCalls.add(AggregateCall.create(
                        aggFn, false, false, false,
                        List.of(), origCall.getArgList(), -1, null,
                        org.apache.calcite.rel.RelCollations.EMPTY,
                        colType,
                        origCall.name + iField.getName()
                    ));
                }
            } else {
                partialCalls.add(origCall.withName(origCall.name));
            }
        }

        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(), partialTraits, child,
            aggregate.getGroupSet(), aggregate.getGroupSets(),
            partialCalls, AggregateMode.PARTIAL, aggregate.getViableBackends()
        );

        RelTraitSet singletonTraits = partial.getTraitSet().replace(context.getDistributionTraitDef().singleton());
        RelNode gathered = convert(partial, singletonTraits);

        // Build final aggCalls: keep original calls adapted to reference partial output columns.
        // DataFusion handles the partial→final merge internally.
        List<AggregateCall> finalCalls = new ArrayList<>();
        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            AggregateCall origCall = aggregate.getAggCallList().get(i);
            int colIdx = groupCount + partialStart[i];
            finalCalls.add(origCall.adaptTo(gathered, List.of(colIdx), origCall.filterArg, groupCount, groupCount));
        }

        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(), singletonTraits, gathered,
            aggregate.getGroupSet(), aggregate.getGroupSets(),
            finalCalls, AggregateMode.FINAL, aggregate.getViableBackends()
        );

        call.transformTo(finalAggregate);
    }

    /** Converts an Arrow type to a nullable Calcite type. */
    private static org.apache.calcite.rel.type.RelDataType arrowToCalcite(
        org.apache.arrow.vector.types.pojo.ArrowType arrowType,
        RelDataTypeFactory typeFactory
    ) {
        if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int i && i.getBitWidth() == 64) {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
        if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
        return typeFactory.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE);
    }
}

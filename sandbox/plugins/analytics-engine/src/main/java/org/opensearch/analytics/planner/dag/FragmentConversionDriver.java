/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Drives fragment conversion for all {@link StagePlan} alternatives in a {@link QueryDAG}.
 * Strips annotations from each resolved fragment and dispatches to the backend's
 * {@link FragmentConvertor} using composable calls — backends never traverse the plan.
 *
 * <p>Dispatch logic for PR2 (pure shard-scan path):
 * <ul>
 *   <li>Leaf = {@link OpenSearchTableScan}, top = {@link OpenSearchAggregate}(PARTIAL):
 *       {@code convertShardScanFragment} on everything below partial agg,
 *       then {@code attachPartialAggOnTop}</li>
 *   <li>Leaf = {@link OpenSearchTableScan}, top = anything else:
 *       {@code convertShardScanFragment} on the full fragment</li>
 *   <li>Leaf = {@link OpenSearchStageInputScan} (reduce stage):
 *       {@code convertFinalAggFragment} on the final agg (ExchangeReducer stripped),
 *       then {@code attachFragmentOnTop} for any operators above it</li>
 * </ul>
 *
 * <p>TODO: as shuffle joins/aggregates and delegation are added, introduce a
 * {@code FragmentConversionStrategy} abstraction so each shape encapsulates its own
 * dispatch logic rather than growing this class with more {@code instanceof} checks.
 *
 * @opensearch.internal
 */
public class FragmentConversionDriver {

    private static final Logger LOGGER = LogManager.getLogger(FragmentConversionDriver.class);

    private FragmentConversionDriver() {}

    /**
     * Converts all {@link StagePlan} alternatives in the DAG, populating
     * {@link StagePlan#convertedBytes()} on each plan.
     */
    public static void convertAll(QueryDAG dag, CapabilityRegistry registry) {
        convertStage(dag.rootStage(), registry);
        // Root stage executes locally at coordinator — store factory for instruction dispatch.
        Stage root = dag.rootStage();
        if (root.getExchangeSinkProvider() != null && !root.getPlanAlternatives().isEmpty()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(root.getPlanAlternatives().getFirst().backendId());
            root.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
        }
    }

    private static void convertStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            convertStage(child, registry);
        }
        List<StagePlan> converted = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
            FragmentConvertor convertor = backend.getFragmentConvertor();

            // Derive filter tree shape BEFORE stripping (annotations must be intact)
            OpenSearchFilter filter = RelNodeUtils.findNode(plan.resolvedFragment(), OpenSearchFilter.class);
            FilterTreeShape treeShape = filter != null
                ? FilterTreeShapeDeriver.derive(filter, plan.backendId())
                : FilterTreeShape.NO_DELEGATION;

            IntraOperatorDelegationBytes delegationBytes = new IntraOperatorDelegationBytes(registry);
            byte[] bytes = convert(plan.resolvedFragment(), convertor, delegationBytes);

            // Assemble instruction list
            List<InstructionNode> instructions = assembleInstructions(backend, plan, treeShape, delegationBytes);

            converted.add(plan.withConvertedBytes(bytes, delegationBytes.getResult()).withInstructions(instructions));
        }
        stage.setPlanAlternatives(converted);
        // Store factory on coordinator-reduce stages (local execution, no serialization needed).
        // Shard stages get the factory from the local backend plugin at the data node.
        if (stage.getExchangeSinkProvider() != null && !converted.isEmpty()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(converted.getFirst().backendId());
            stage.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
        }
    }

    private static List<InstructionNode> assembleInstructions(
        AnalyticsSearchBackendPlugin backend,
        StagePlan plan,
        FilterTreeShape treeShape,
        IntraOperatorDelegationBytes delegationBytes
    ) {
        FragmentInstructionHandlerFactory factory = backend.getInstructionHandlerFactory();
        LinkedList<InstructionNode> instructions = new LinkedList<>();
        RelNode leaf = findLeaf(plan.resolvedFragment());

        if (leaf instanceof OpenSearchTableScan) {
            factory.createShardScanNode().ifPresent(instructions::add);
            List<DelegatedExpression> delegated = delegationBytes.getResult();
            if (!delegated.isEmpty()) {
                factory.createFilterDelegationNode(treeShape, delegated.size(), delegated).ifPresent(instructions::add);
            }
            if (plan.resolvedFragment() instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL) {
                factory.createPartialAggregateNode().ifPresent(instructions::add);
            }
        } else if (leaf instanceof OpenSearchStageInputScan) {
            if (hasAggregate(plan.resolvedFragment())) {
                factory.createFinalAggregateNode().ifPresent(instructions::add);
            }
        }

        return instructions;
    }

    /**
     * Lazily accumulates serialized delegated query bytes during fragment conversion.
     * Only allocates the map when the first delegated annotation is encountered.
     */
    static final class IntraOperatorDelegationBytes {
        private final CapabilityRegistry registry;
        private List<DelegatedExpression> delegatedExpressions;

        IntraOperatorDelegationBytes(CapabilityRegistry registry) {
            this.registry = registry;
        }

        /**
         * Creates an annotation resolver scoped to a specific operator. Compares each
         * annotation's viable backend against the operator's backend: native annotations
         * are unwrapped, delegated ones are serialized and replaced with a placeholder.
         */
        Function<OperatorAnnotation, RexNode> resolverFor(OpenSearchRelNode operator, RexBuilder rexBuilder) {
            String operatorBackend = operator.getViableBackends().getFirst();
            List<FieldStorageInfo> fieldStorage = operator.getOutputFieldStorage();
            return annotation -> {
                String annotationBackend = annotation.getViableBackends().getFirst();
                if (annotationBackend.equals(operatorBackend)) {
                    LOGGER.debug("Native annotation [id={}]: backend [{}] matches operator", annotation.getAnnotationId(), operatorBackend);
                    return annotation.unwrap();
                }
                RexNode original = annotation.unwrap();
                if (!(original instanceof RexCall originalCall) || !(originalCall.getOperator() instanceof SqlFunction sqlFunction)) {
                    throw new IllegalStateException("Delegated expression must be a SqlFunction call: " + original);
                }
                ScalarFunction function = ScalarFunction.fromSqlFunction(sqlFunction);
                DelegatedPredicateSerializer serializer = registry.getBackend(annotationBackend)
                    .getCapabilityProvider()
                    .delegatedPredicateSerializers()
                    .get(function);
                if (serializer == null) {
                    throw new IllegalStateException(
                        "No DelegatedPredicateSerializer for ["
                            + function
                            + "] on backend ["
                            + annotationBackend
                            + "]. CapabilityRegistry should have rejected this at startup."
                    );
                }
                byte[] serialized = serializer.serialize(originalCall, fieldStorage);
                LOGGER.debug(
                    "Delegated annotation [id={}]: {} from operator [{}] to [{}], serialized {} bytes",
                    annotation.getAnnotationId(),
                    function,
                    operatorBackend,
                    annotationBackend,
                    serialized.length
                );
                if (delegatedExpressions == null) {
                    delegatedExpressions = new ArrayList<>();
                }
                delegatedExpressions.add(new DelegatedExpression(annotation.getAnnotationId(), annotationBackend, serialized));
                return annotation.makePlaceholder(rexBuilder);
            };
        }

        List<DelegatedExpression> getResult() {
            return delegatedExpressions != null ? delegatedExpressions : List.of();
        }
    }

    /**
     * Dispatches conversion based on the fragment's leaf and top node types.
     */
    static byte[] convert(RelNode resolvedFragment, FragmentConvertor convertor, IntraOperatorDelegationBytes delegationBytes) {
        RelNode leaf = findLeaf(resolvedFragment);

        if (leaf instanceof OpenSearchTableScan scan) {
            String tableName = scan.getTable().getQualifiedName().getLast();

            // Partial agg at top: convert everything below it, then attach partial agg on top.
            // strippedInputs passed to stripAnnotations for schema validity (LogicalAggregate needs its inputs).
            if (resolvedFragment instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL) {
                List<RelNode> strippedInputs = agg.getInputs().stream().map(input -> strip(input, delegationBytes)).toList();
                byte[] innerBytes = convertor.convertShardScanFragment(tableName, strippedInputs.getFirst());
                Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(agg, agg.getCluster().getRexBuilder());
                RelNode strippedAgg = agg.stripAnnotations(strippedInputs, resolver);
                return convertor.attachPartialAggOnTop(strippedAgg, innerBytes);
            }

            RelNode stripped = strip(resolvedFragment, delegationBytes);
            return convertor.convertShardScanFragment(tableName, stripped);
        }

        if (leaf instanceof OpenSearchStageInputScan) {
            return convertReduceFragment(resolvedFragment, convertor, delegationBytes);
        }

        throw new IllegalStateException(
            "Unknown leaf type [" + leaf.getClass().getSimpleName() + "]. " + "Add a FragmentConversionStrategy for this leaf type."
        );
    }

    /**
     * Reduce stage conversion: strips ExchangeReducer, converts the final agg fragment
     * (with StageInputScan as leaf for schema), then attaches any operators above it
     * (Sort, Project, etc.) via attachFragmentOnTop.
     *
     * The node immediately above ExchangeReducer is the final agg — it goes to
     * convertFinalAggFragment together with StageInputScan. Only operators strictly
     * above the final agg use attachFragmentOnTop.
     *
     * TODO: for joins, the coordinator fragment has a join node directly above two
     * StageInputScan leaves (no ExchangeReducer between them). convertReduceNode
     * currently only recognizes the ExchangeReducer boundary — add join handling
     * when shuffle joins are implemented (check if all inputs are StageInputScan
     * and dispatch to a dedicated convertJoinFragment method).
     */
    private static byte[] convertReduceFragment(RelNode node, FragmentConvertor convertor, IntraOperatorDelegationBytes delegationBytes) {
        return convertReduceNode(node, convertor, false, delegationBytes);
    }

    private static byte[] convertReduceNode(
        RelNode node,
        FragmentConvertor convertor,
        boolean finalAggConverted,
        IntraOperatorDelegationBytes delegationBytes
    ) {
        if (node instanceof OpenSearchExchangeReducer) {
            // Strip ExchangeReducer — StageInputScan below it is the schema source
            // This should never be reached directly; handled by the parent (final agg)
            return convertor.convertFinalAggFragment(strip(node.getInputs().getFirst(), delegationBytes));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            List<RelNode> strippedInputs = node.getInputs().stream().map(input -> strip(input, delegationBytes)).toList();
            Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(openSearchNode, node.getCluster().getRexBuilder());
            RelNode strippedNode = openSearchNode.stripAnnotations(strippedInputs, resolver);

            if (!finalAggConverted) {
                // First OpenSearchRelNode whose ALL inputs are ExchangeReducers is treated as the
                // boundary between the coordinator-side fragment and the data-node child stages.
                // For single-input shapes (Sort/Project/Aggregate over a partial agg) this is the
                // final-aggregate operator; for multi-input shapes (Union) every branch is itself
                // an ER → StageInputScan, and the entire Union+ER subtree is converted as one
                // fragment so all branches end up in the same Substrait plan reading from their
                // respective input partitions.
                boolean allChildrenAreExchangeReducer = !node.getInputs().isEmpty()
                    && node.getInputs().stream().allMatch(input -> input instanceof OpenSearchExchangeReducer);
                if (allChildrenAreExchangeReducer) {
                    List<RelNode> finalAggInputs = new ArrayList<>(node.getInputs().size());
                    for (RelNode input : node.getInputs()) {
                        // Skip the ER, keep StageInputScan below it as the leaf for schema inference.
                        finalAggInputs.add(strip(input.getInputs().getFirst(), delegationBytes));
                    }
                    RelNode finalAggFragment = openSearchNode.stripAnnotations(finalAggInputs, resolver);
                    return convertor.convertFinalAggFragment(finalAggFragment);
                }
            }

            // Operator above the final-fragment boundary — convert child first, then attach.
            byte[] innerBytes = convertReduceNode(node.getInputs().getFirst(), convertor, false, delegationBytes);
            return convertor.attachFragmentOnTop(strippedNode, innerBytes);
        }
        throw new IllegalStateException("Unexpected reduce stage node: " + node.getClass().getSimpleName());
    }

    /** Recursively strips annotations bottom-up. Keeps OpenSearchStageInputScan as-is. */
    private static RelNode strip(RelNode node, IntraOperatorDelegationBytes delegationBytes) {
        if (node instanceof OpenSearchStageInputScan) {
            return node; // kept for schema inference at reduce stage
        }
        if (node instanceof OpenSearchExchangeReducer) {
            return strip(node.getInputs().getFirst(), delegationBytes);
        }
        List<RelNode> strippedChildren = new ArrayList<>(node.getInputs().size());
        for (RelNode input : node.getInputs()) {
            strippedChildren.add(strip(input, delegationBytes));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(openSearchNode, node.getCluster().getRexBuilder());
            return openSearchNode.stripAnnotations(strippedChildren, resolver);
        }
        return node;
    }

    /**
     * Rewrites the StageInputScan row type to match the actual partial-state schema
     * for aggregate calls with declared intermediateFields. Also rewrites the aggregate
     * to SUM the intermediate columns and adds a Project with the finalExpression.
     */
    private static RelNode fixIntermediateInputTypes(RelNode node) {
        if (!(node instanceof org.apache.calcite.rel.core.Aggregate agg)) {
            if (node.getInputs().size() == 1) {
                return node.copy(node.getTraitSet(), List.of(fixIntermediateInputTypes(node.getInputs().get(0))));
            }
            return node;
        }
        RelNode child = agg.getInput();
        if (!(child instanceof OpenSearchStageInputScan scan)) return node;

        RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
        RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
        int groupCount = agg.getGroupSet().cardinality();

        List<String> scanNames = new ArrayList<>();
        List<org.apache.calcite.rel.type.RelDataType> scanTypes = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            RelDataTypeField f = scan.getRowType().getFieldList().get(i);
            scanNames.add(f.getName());
            scanTypes.add(f.getType());
        }

        List<Integer> partialStart = new ArrayList<>();
        List<BiFunction<RexBuilder, List<RexNode>, RexNode>> finalExprs = new ArrayList<>();
        boolean needsRewrite = false;

        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCall call = agg.getAggCallList().get(i);
            RelDataTypeField f = scan.getRowType().getFieldList().get(groupCount + i);
            AggregateFunction func = AggregateFunction.fromAggregateCall(call);
            List<Field> iFields = func != null ? func.getIntermediateFields() : null;
            partialStart.add(scanNames.size());
            if (iFields != null) {
                needsRewrite = true;
                for (Field iField : iFields) {
                    String suffix = iField.getName();
                    scanNames.add(suffix.isEmpty() ? f.getName() : f.getName() + suffix);
                    scanTypes.add(arrowTypeToCalcite(iField.getFieldType().getType(), typeFactory));
                }
                finalExprs.add(func.getFinalExpression());
            } else {
                needsRewrite = true;
                scanNames.add(f.getName());
                // SUM of a NOT NULL column infers nullable (empty group → NULL), so we must
                // store the type as nullable to satisfy Calcite's typeMatchesInferred check.
                scanTypes.add(typeFactory.createTypeWithNullability(f.getType(), true));
                finalExprs.add((rb, refs) -> refs.get(0));
            }
        }
        if (!needsRewrite) return node;

        var newScan = scan.withRowType(typeFactory.createStructType(scanTypes, scanNames));

        List<AggregateCall> newAggCalls = new ArrayList<>();
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCall call = agg.getAggCallList().get(i);
            AggregateFunction func = AggregateFunction.fromAggregateCall(call);
            List<Field> iFields = func != null ? func.getIntermediateFields() : null;
            int start = partialStart.get(i);
            if (iFields != null) {
                var finalExpr = finalExprs.get(i);
                if (finalExpr != null) {
                    // SUM each intermediate field; finalExpr combines them in the Project
                    for (int j = 0; j < iFields.size(); j++) {
                        int colIdx = start + j;
                        var inputType = scanTypes.get(colIdx);
                        newAggCalls.add(
                            AggregateCall.create(
                                SqlStdOperatorTable.SUM,
                                false,
                                List.of(colIdx),
                                -1,
                                groupCount,
                                newScan,
                                sumReturnType(inputType, typeFactory),
                                scanNames.get(colIdx)
                            )
                        );
                    }
                } else {
                    // No finalExpr (e.g. DC): keep original call adapted to new scan column
                    newAggCalls.add(call.adaptTo(newScan, List.of(start), call.filterArg, groupCount, agg.getGroupCount()));
                }
            } else {
                var inputType = scanTypes.get(start);
                newAggCalls.add(
                    AggregateCall.create(
                        SqlStdOperatorTable.SUM,
                        false,
                        List.of(start),
                        -1,
                        groupCount,
                        newScan,
                        sumReturnType(inputType, typeFactory),
                        scanNames.get(start)
                    )
                );
            }
        }

        LogicalAggregate newAgg = new LogicalAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getHints(),
            newScan,
            agg.getGroupSet(),
            agg.getGroupSets(),
            newAggCalls
        );

        boolean needsProject = finalExprs.stream().anyMatch(e -> e != null);
        if (!needsProject) return newAgg;

        List<RexNode> projectExprs = new ArrayList<>();
        List<String> projectNames = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            projectExprs.add(rexBuilder.makeInputRef(newAgg, i));
            projectNames.add(newAgg.getRowType().getFieldList().get(i).getName());
        }
        int aggColIdx = groupCount;
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCall origCall = agg.getAggCallList().get(i);
            AggregateFunction func = AggregateFunction.fromAggregateCall(origCall);
            List<Field> iFields = func != null ? func.getIntermediateFields() : null;
            var finalExpr = finalExprs.get(i);
            if (iFields != null && finalExpr != null) {
                List<RexNode> partialRefs = new ArrayList<>();
                for (int j = 0; j < iFields.size(); j++) {
                    partialRefs.add(rexBuilder.makeInputRef(newAgg, aggColIdx + j));
                }
                projectExprs.add(finalExpr.apply(rexBuilder, partialRefs));
                aggColIdx += iFields.size();
            } else if (iFields != null) {
                projectExprs.add(rexBuilder.makeInputRef(newAgg, aggColIdx));
                aggColIdx += iFields.size();
            } else {
                projectExprs.add(rexBuilder.makeInputRef(newAgg, aggColIdx));
                aggColIdx += 1;
            }
            projectNames.add(origCall.name != null ? origCall.name : agg.getRowType().getFieldList().get(groupCount + i).getName());
        }
        org.apache.calcite.rel.type.RelDataType projectRowType = typeFactory.createStructType(
            projectExprs.stream().map(RexNode::getType).toList(),
            projectNames
        );
        return new LogicalProject(agg.getCluster(), agg.getTraitSet(), List.of(), newAgg, projectExprs, projectRowType);
    }

    private static org.apache.calcite.rel.type.RelDataType arrowTypeToCalcite(ArrowType arrowType, RelDataTypeFactory f) {
        if (arrowType instanceof ArrowType.Int i && i.getBitWidth() == 64) {
            return f.createSqlType(SqlTypeName.BIGINT);
        }
        if (arrowType instanceof ArrowType.FloatingPoint) {
            return f.createSqlType(SqlTypeName.DOUBLE);
        }
        return f.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE);
    }

    private static boolean hasAggregate(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.Aggregate) return true;
        if (node.getInputs().size() == 1) return hasAggregate(node.getInputs().get(0));
        return false;
    }

    /**
     * Mirrors Calcite's SUM return-type inference: integer inputs → BIGINT, floating → DOUBLE,
     * preserving the input's nullability. This avoids the BIGINT vs BIGINT NOT NULL mismatch
     * that occurs when passing an explicit type that doesn't match Calcite's inference.
     */
    private static org.apache.calcite.rel.type.RelDataType sumReturnType(
        org.apache.calcite.rel.type.RelDataType inputType,
        RelDataTypeFactory typeFactory
    ) {
        org.apache.calcite.rel.type.RelDataType base;
        switch (inputType.getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                base = typeFactory.createSqlType(SqlTypeName.BIGINT);
                break;
            case FLOAT:
                base = typeFactory.createSqlType(SqlTypeName.FLOAT);
                break;
            default:
                base = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
        return typeFactory.createTypeWithNullability(base, inputType.isNullable());
    }

    private static RelNode findLeaf(RelNode node) {
        if (node.getInputs().isEmpty()) {
            return node;
        }
        return findLeaf(node.getInputs().getFirst());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;

/**
 * Converts Calcite RelNode fragments to Substrait protobuf bytes
 * for the DataFusion Rust runtime.
 *
 * <p>Dispatch summary:
 * <ul>
 *   <li>{@link #convertShardScanFragment(String, RelNode)} and
 *       {@link #convertFinalAggFragment(RelNode)} — full-fragment conversions via
 *       {@link #convertToSubstrait(RelNode)}.</li>
 *   <li>{@link #attachPartialAggOnTop(RelNode, byte[])} and
 *       {@link #attachFragmentOnTop(RelNode, byte[])} — convert the wrapping
 *       operator standalone, then rewire its input to the decoded inner plan's
 *       root via {@link #rewire(Plan, Rel)}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor, org.opensearch.analytics.planner.RegistryAware {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    /** Renames Substrait function names to match DataFusion's expected names. */
    private static final java.util.Map<String, String> FUNCTION_RENAMES = java.util.Map.of("approx_count_distinct", "approx_distinct");

    /**
     * Maps backend-specific Calcite operators to their Substrait extension names so Isthmus
     * serializes them through our {@code SimpleExtension} catalog:
     * <ul>
     *   <li>{@link DelegatedPredicateFunction} → {@code delegated_predicate} (delegation to a peer backend).</li>
     *   <li>{@link SqlLibraryOperators#ILIKE} → {@code ilike} (case-insensitive LIKE; resolved by
     *       DataFusion's substrait consumer to a case-insensitive {@code LikeExpr}).</li>
     *   <li>{@link SqlLibraryOperators#REGEXP_CONTAINS} → {@code regex_match} (boolean regex match;
     *       resolved by DataFusion's substrait consumer to {@code Operator::RegexMatch}, the same
     *       binary operator that backs PostgreSQL's {@code ~} regex match). Lowering target for PPL
     *       {@code regex} command and {@code regexp_match()} function.</li>
     *   <li>{@link SqlStdOperatorTable#REPLACE} → {@code replace} (literal string replacement;
     *       lowering target for PPL `replace` command on non-wildcard patterns).</li>
     *   <li>{@link SqlLibraryOperators#REGEXP_REPLACE_3} → {@code regexp_replace} (regex string
     *       replacement; lowering target for PPL `replace` command on wildcard patterns and for
     *       PPL `replace()` / `regexp_replace()` functions in `eval`).</li>
     * </ul>
     */
    private static final List<FunctionMappings.Sig> ADDITIONAL_SCALAR_SIGS = List.of(
        FunctionMappings.s(DelegatedPredicateFunction.FUNCTION, DelegatedPredicateFunction.NAME),
        FunctionMappings.s(SqlLibraryOperators.ILIKE, "ilike"),
        FunctionMappings.s(DelegatedPredicateFunction.FUNCTION, DelegatedPredicateFunction.NAME),
        FunctionMappings.s(SqlLibraryOperators.DATE_PART, "date_part"),
        FunctionMappings.s(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, "convert_tz"),
        FunctionMappings.s(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, "to_unixtime"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_CONTAINS, "regex_match"),
        FunctionMappings.s(SqlStdOperatorTable.REPLACE, "replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_3, "regexp_replace")
    );

    private final SimpleExtension.ExtensionCollection extensions;
    private org.opensearch.analytics.planner.CapabilityRegistry capabilityRegistry;
    private String backendId;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public void setRegistry(org.opensearch.analytics.planner.CapabilityRegistry registry, String backendId) {
        this.capabilityRegistry = registry;
        this.backendId = backendId;
    }

    @Override
    public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
        LOGGER.debug("Converting shard scan fragment for table [{}]", tableName);
        return convertToSubstrait(fragment);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        LOGGER.debug("Attaching partial aggregate on top of {} inner bytes", innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        Rel wrapper = convertStandalone(partialAggFragment);
        Plan rewired = rewire(inner, wrapper);
        return serializePlan(rewired);
    }

    @Override
    public byte[] convertFinalAggFragment(RelNode fragment) {
        LOGGER.debug("Converting final-aggregate fragment");
        RelNode fixed = fixIntermediateInputTypes(fragment);
        RelNode rewritten = rewriteStageInputScans(fixed);
        return convertToSubstrait(rewritten);
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LOGGER.debug("Attaching generic fragment [{}] on top of {} inner bytes", fragment.getClass().getSimpleName(), innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        // Rewrite OpenSearchStageInputScans before standalone conversion so the isthmus
        // visitor can traverse the fragment without choking on planner-internal leaves.
        // The standalone conversion's children are discarded by rewire(...) anyway, but
        // the visitor still walks them top-down to build the wrapper rel.
        RelNode rewritten = rewriteStageInputScans(fragment);
        Rel wrapper = convertStandalone(rewritten);
        return serializePlan(rewire(inner, wrapper));
    }

    // ── Core conversion helpers ─────────────────────────────────────────────────

    private byte[] convertToSubstrait(RelNode fragment) {
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = SubstraitPlanRewriter.rewrite(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        protoPlan = renameExtensionFunctions(protoPlan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    /**
     * Converts a single operator into a Substrait {@link Rel}. The operator may carry
     * children (e.g. the {@code attachPartialAggOnTop} caller passes a
     * {@code LogicalAggregate} whose input is the already-stripped inner tree); we
     * deliberately discard those children by taking only the outermost rel of the
     * conversion and rewiring its input during {@link #rewire(Plan, Rel)}.
     */
    private Rel convertStandalone(RelNode operator) {
        SubstraitRelVisitor visitor = createVisitor(operator);
        return visitor.apply(operator);
    }

    /**
     * Rewires the Substrait {@code wrapper} rel to sit above the root relation of
     * {@code inner}. Returns a new {@link Plan} whose single root is
     * {@code wrapper(inner.root)}. Supports the known single-input wrappers emitted
     * by our four SPI methods ({@link Aggregate}, {@link Sort}, {@link Filter},
     * {@link Project}).
     */
    static Plan rewire(Plan inner, Rel wrapper) {
        if (inner.getRoots().isEmpty()) {
            throw new IllegalArgumentException("Inner Substrait plan has no root relation to rewire under wrapper");
        }
        Plan.Root innerRoot = inner.getRoots().get(0);
        Rel innerRel = innerRoot.getInput();
        Rel rewired = replaceInput(wrapper, innerRel);
        return Plan.builder()
            .addRoots(Plan.Root.builder().input(rewired).names(deriveNames(rewired, innerRoot.getNames())).build())
            .build();
    }

    private static List<String> deriveNames(Rel rel, List<String> innerNames) {
        if (rel instanceof Aggregate agg) {
            List<String> names = new ArrayList<>();
            for (io.substrait.expression.Expression expr : agg.getGroupings().stream().flatMap(g -> g.getExpressions().stream()).toList()) {
                names.add("group_" + names.size());
            }
            for (Aggregate.Measure m : agg.getMeasures()) {
                names.add("agg_" + names.size());
            }
            return names;
        }
        if (rel instanceof Project proj) {
            List<String> names = new ArrayList<>();
            for (int i = 0; i < proj.getExpressions().size(); i++) {
                names.add("proj_" + i);
            }
            return names;
        }
        return innerNames;
    }

    private static Rel replaceInput(Rel wrapper, Rel newInput) {
        if (wrapper instanceof Aggregate agg) {
            return Aggregate.builder().from(agg).input(newInput).build();
        }
        if (wrapper instanceof Sort sort) {
            return Sort.builder().from(sort).input(newInput).build();
        }
        if (wrapper instanceof Filter filter) {
            return Filter.builder().from(filter).input(newInput).build();
        }
        if (wrapper instanceof Project project) {
            return Project.builder().from(project).input(newInput).build();
        }
        if (wrapper instanceof Fetch fetch) {
            // SystemLimit + LogicalSort with offset/fetch lower to a Substrait Fetch rel.
            // Used by the implicit query-size limit at the top of every analytics-engine plan
            // and by user-level `head N` clauses; both arrive here when attached above a Union.
            return Fetch.builder().from(fetch).input(newInput).build();
        }
        throw new UnsupportedOperationException(
            "Cannot attach-on-top a Substrait Rel of type " + wrapper.getClass().getSimpleName() + " — no single-input rewire defined"
        );
    }

    /**
     * Overrides the {@link Expression.AggregationPhase} on every {@link Aggregate.Measure}
     * inside an {@link Aggregate} wrapper. No-op for non-aggregate wrappers.
     *
     * <p>Isthmus hardcodes {@code INITIAL_TO_RESULT} on every aggregate-function
     * invocation. For the partial-agg-attach-on-shard path we want
     * {@code INITIAL_TO_INTERMEDIATE}; the final-agg path stays at
     * {@code INITIAL_TO_RESULT} (isthmus's default) which the DataFusion
     * substrait deserialiser treats as the single-stage/final form.
     */
    private static Rel withAggregationPhase(Rel rel, Expression.AggregationPhase phase) {
        if (!(rel instanceof Aggregate agg)) {
            return rel;
        }
        List<Aggregate.Measure> newMeasures = new ArrayList<>(agg.getMeasures().size());
        for (Aggregate.Measure m : agg.getMeasures()) {
            AggregateFunctionInvocation fn = m.getFunction();
            AggregateFunctionInvocation rephased = AggregateFunctionInvocation.builder().from(fn).aggregationPhase(phase).build();
            newMeasures.add(Aggregate.Measure.builder().from(m).function(rephased).build());
        }
        return Aggregate.builder().from(agg).measures(newMeasures).build();
    }

    /**
     * Rewrites every {@link OpenSearchStageInputScan} in the RelNode tree to a plain
     * Calcite {@link TableScan} whose qualified name matches what the matching
     * {@link DatafusionReduceSink} input partition registers on the native session.
     *
     * <p>The table id is {@code "input-<childStageId>"}, mirroring
     * {@code AbstractDatafusionReduceSink.inputIdFor}. For a single-input fragment the
     * sole stage id (typically 0) reproduces the conventional {@code "input-0"} name; for
     * multi-input shapes (Union) each branch refers to its own child stage id and the
     * isthmus visitor emits one {@link NamedScan} per branch.
     */

    /**
     * Rewrites the StageInputScan row type to match the actual partial-state schema
     * for aggregate calls with declared intermediateFields.
     */
    /**
     * Fixes the StageInputScan row type so it matches the actual Arrow schema of the
     * streaming table (which uses intermediateFields for approximate aggregates).
     * The aggregate itself is unchanged — force_aggregate_mode(Final) in Rust handles merging.
     */
    private RelNode fixIntermediateInputTypes(RelNode node) {
        if (!(node instanceof org.apache.calcite.rel.core.Aggregate agg)) {
            if (node.getInputs().size() == 1) {
                return node.copy(node.getTraitSet(), List.of(fixIntermediateInputTypes(node.getInputs().get(0))));
            }
            return node;
        }
        RelNode child = agg.getInput();
        if (!(child instanceof OpenSearchStageInputScan scan)) return node;
        if (capabilityRegistry == null || backendId == null) return node;

        RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
        org.apache.calcite.rex.RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
        int groupCount = agg.getGroupSet().cardinality();

        List<String> scanNames = new ArrayList<>();
        List<RelDataType> scanTypes = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            RelDataTypeField f = scan.getRowType().getFieldList().get(i);
            scanNames.add(f.getName());
            scanTypes.add(f.getType());
        }

        List<Integer> partialStart = new ArrayList<>();
        List<
            java.util.function.BiFunction<
                org.apache.calcite.rex.RexBuilder,
                List<org.apache.calcite.rex.RexNode>,
                org.apache.calcite.rex.RexNode>> finalExprs = new ArrayList<>();
        boolean needsRewrite = false;

        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            org.apache.calcite.rel.core.AggregateCall call = agg.getAggCallList().get(i);
            RelDataTypeField f = scan.getRowType().getFieldList().get(groupCount + i);
            org.opensearch.analytics.spi.AggregateFunction func = org.opensearch.analytics.spi.AggregateFunction.fromAggregateCall(call);
            List<org.apache.arrow.vector.types.pojo.Field> iFields = func != null
                ? capabilityRegistry.getIntermediateFields(backendId, func)
                : null;
            partialStart.add(scanNames.size());
            if (iFields != null) {
                needsRewrite = true;
                for (org.apache.arrow.vector.types.pojo.Field iField : iFields) {
                    String suffix = iField.getName();
                    scanNames.add(suffix.isEmpty() ? f.getName() : f.getName() + suffix);
                    scanTypes.add(arrowTypeToCalcite(iField.getFieldType().getType(), typeFactory));
                }
                finalExprs.add(capabilityRegistry.getFinalExpression(backendId, func));
            } else {
                scanNames.add(f.getName());
                scanTypes.add(f.getType());
                finalExprs.add(null);
            }
        }
        if (!needsRewrite) return node;

        OpenSearchStageInputScan newScan = scan.withRowType(typeFactory.createStructType(scanTypes, scanNames));

        List<org.apache.calcite.rel.core.AggregateCall> newAggCalls = new ArrayList<>();
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            org.apache.calcite.rel.core.AggregateCall call = agg.getAggCallList().get(i);
            org.opensearch.analytics.spi.AggregateFunction func = org.opensearch.analytics.spi.AggregateFunction.fromAggregateCall(call);
            List<org.apache.arrow.vector.types.pojo.Field> iFields = func != null
                ? capabilityRegistry.getIntermediateFields(backendId, func)
                : null;
            int start = partialStart.get(i);
            if (iFields != null && finalExprs.get(i) != null) {
                for (int j = 0; j < iFields.size(); j++) {
                    RelDataType colType = typeFactory.createTypeWithNullability(scanTypes.get(start + j), true);
                    newAggCalls.add(
                        org.apache.calcite.rel.core.AggregateCall.create(
                            io.substrait.isthmus.AggregateFunctions.SUM,
                            false,
                            false,
                            false,
                            List.of(),
                            List.of(start + j),
                            -1,
                            null,
                            org.apache.calcite.rel.RelCollations.EMPTY,
                            colType,
                            scanNames.get(start + j)
                        )
                    );
                }
            } else {
                newAggCalls.add(call.adaptTo(newScan, List.of(start), call.filterArg, groupCount, agg.getGroupCount()));
            }
        }

        org.apache.calcite.rel.logical.LogicalAggregate newAgg = new org.apache.calcite.rel.logical.LogicalAggregate(
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

        List<org.apache.calcite.rex.RexNode> projectExprs = new ArrayList<>();
        List<String> projectNames = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            projectExprs.add(rexBuilder.makeInputRef(newAgg, i));
            projectNames.add(newAgg.getRowType().getFieldList().get(i).getName());
        }
        int aggColIdx = groupCount;
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            org.apache.calcite.rel.core.AggregateCall origCall = agg.getAggCallList().get(i);
            org.opensearch.analytics.spi.AggregateFunction func = org.opensearch.analytics.spi.AggregateFunction.fromAggregateCall(
                origCall
            );
            List<org.apache.arrow.vector.types.pojo.Field> iFields = func != null
                ? capabilityRegistry.getIntermediateFields(backendId, func)
                : null;
            var finalExpr = finalExprs.get(i);
            if (iFields != null && finalExpr != null) {
                List<org.apache.calcite.rex.RexNode> partialRefs = new ArrayList<>();
                for (int j = 0; j < iFields.size(); j++) {
                    partialRefs.add(rexBuilder.makeInputRef(newAgg, aggColIdx + j));
                }
                projectExprs.add(finalExpr.apply(rexBuilder, partialRefs));
                aggColIdx += iFields.size();
            } else {
                projectExprs.add(rexBuilder.makeInputRef(newAgg, aggColIdx));
                aggColIdx += 1;
            }
            projectNames.add(origCall.name != null ? origCall.name : agg.getRowType().getFieldList().get(groupCount + i).getName());
        }
        RelDataType projectRowType = typeFactory.createStructType(
            projectExprs.stream().map(org.apache.calcite.rex.RexNode::getType).toList(),
            projectNames
        );
        return new org.apache.calcite.rel.logical.LogicalProject(
            agg.getCluster(),
            agg.getTraitSet(),
            List.of(),
            newAgg,
            projectExprs,
            projectRowType
        );
    }

    private static RelDataType arrowTypeToCalcite(org.apache.arrow.vector.types.pojo.ArrowType arrowType, RelDataTypeFactory f) {
        if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int i && i.getBitWidth() == 64) {
            return f.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
        }
        if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
            return f.createSqlType(org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
        }
        return f.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARBINARY, Integer.MAX_VALUE);
    }

    private static RelNode rewriteStageInputScans(RelNode node) {
        if (node instanceof OpenSearchStageInputScan scan) {
            return new StageInputTableScan(scan.getCluster(), scan.getTraitSet(), "input-" + scan.getChildStageId(), scan.getRowType());
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteStageInputScans(input);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        if (changed) {
            return node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    // ── Visitor wiring ──────────────────────────────────────────────────────────

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(),
            ADDITIONAL_SCALAR_SIGS,
            typeFactory,
            typeConverter
        );
        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory) {
            @Override
            protected FunctionFinder getFunctionFinder(org.apache.calcite.rel.core.AggregateCall call) {
                FunctionFinder finder = super.getFunctionFinder(call);
                if (finder == null && call.getAggregation().getKind() == SqlKind.AVG) {
                    finder = signatures.get(io.substrait.isthmus.AggregateFunctions.AVG);
                }
                return finder;
            }
        };
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);
        ConverterProvider converterProvider = new ConverterProvider(
            typeFactory,
            extensions,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter
        );
        return new SubstraitRelVisitor(converterProvider);
    }

    // ── Plan serde helpers ──────────────────────────────────────────────────────

    /** Decodes serialized Substrait bytes into a model-level {@link Plan}. */
    private Plan decodePlan(byte[] bytes) {
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return new ProtoPlanConverter(extensions).from(proto);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode Substrait plan bytes", e);
        }
    }

    /** Serializes a model-level {@link Plan} to proto bytes. */
    private static byte[] serializePlan(Plan plan) {
        io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);
        proto = renameExtensionFunctions(proto);
        return proto.toByteArray();
    }

    /** Renames Substrait extension function names to match DataFusion's expected names. */
    private static io.substrait.proto.Plan renameExtensionFunctions(io.substrait.proto.Plan plan) {
        boolean changed = false;
        io.substrait.proto.Plan.Builder builder = plan.toBuilder();
        for (int i = 0; i < plan.getExtensionsCount(); i++) {
            io.substrait.proto.SimpleExtensionDeclaration ext = plan.getExtensions(i);
            if (ext.hasExtensionFunction()) {
                String name = ext.getExtensionFunction().getName();
                String base = name.contains(":") ? name.substring(0, name.indexOf(':')) : name;
                String to = FUNCTION_RENAMES.get(base);
                if (to != null) {
                    String newName = to + name.substring(base.length());
                    builder.setExtensions(
                        i,
                        ext.toBuilder().setExtensionFunction(ext.getExtensionFunction().toBuilder().setName(newName)).build()
                    );
                    changed = true;
                }
            }
        }
        return changed ? builder.build() : plan;
    }

    // ── Calcite TableScan wrappers for OpenSearchStageInputScan rewrite ─────────

    /**
     * Minimal {@link TableScan} representing a stage-input source. The backing
     * {@link StageInputRelOptTable} reports the stage-input id as its single qualified
     * name; isthmus converts this to a {@link NamedScan} with that one-element name.
     */
    static final class StageInputTableScan extends TableScan {
        StageInputTableScan(RelOptCluster cluster, RelTraitSet traitSet, String stageInputId, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new StageInputRelOptTable(stageInputId, rowType));
        }
    }

    /**
     * Minimal {@link RelOptTable} implementation — only {@code getQualifiedName()} and
     * {@code getRowType()} are consulted by the isthmus visitor.
     */
    static final class StageInputRelOptTable implements RelOptTable {
        private final List<String> qualifiedName;
        private final RelDataType rowType;

        StageInputRelOptTable(String stageInputId, RelDataType rowType) {
            this.qualifiedName = List.of(stageInputId);
            this.rowType = rowType;
        }

        @Override
        public List<String> getQualifiedName() {
            return qualifiedName;
        }

        @Override
        public RelDataType getRowType() {
            return rowType;
        }

        @Override
        public double getRowCount() {
            return 100;
        }

        @Override
        public RelOptSchema getRelOptSchema() {
            return null;
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            throw new UnsupportedOperationException("StageInputRelOptTable.toRel not supported");
        }

        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            return List.of();
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        @Override
        public List<ImmutableBitSet> getKeys() {
            return List.of();
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        @Override
        public List<RelCollation> getCollationList() {
            return List.of();
        }

        @Override
        public RelDistribution getDistribution() {
            return RelDistributions.ANY;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
            return null;
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            return this;
        }
    }
}

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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.ExecutionMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IndexSettings;

import java.math.BigDecimal;
import java.util.List;

/**
 * Converts {@link Sort} → {@link OpenSearchSort}.
 *
 * <p>Validates the chosen backend supports {@link EngineCapability#SORT}, applies
 * a few structural rewrites (drop redundant outer Sorts, push the system-LIMIT
 * fetch into a collated inner Sort), and validates user-explicit limits against
 * each table's {@code index.max_result_window}.
 *
 * @opensearch.internal
 */
public class OpenSearchSortRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchSortRule(PlannerContext context) {
        super(operand(Sort.class, operand(RelNode.class, any())), "OpenSearchSortRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        RelNode child = call.rel(1);

        if (sort instanceof OpenSearchSort) {
            return;
        }

        if (!(child instanceof OpenSearchRelNode openSearchChild)) {
            throw new IllegalStateException("Sort rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }

        // Drop a pure-Fetch Sort over an Aggregate (possibly via a Project chain). Aggregate
        // output cardinality is bounded by group count, so a JOIN_SUBSEARCH_MAXOUT-style 50000
        // limit is structurally never hit. The Sort/Fetch shape here also triggers a DataFusion
        // hang for composite-group keys. Removing it keeps the safety contract intact for
        // unbounded children but avoids the redundant Fetch when the aggregate already bounds
        // output.
        if (sort.getCollation().getFieldCollations().isEmpty() && sort.offset == null && hasAggregateUnderProjects(child)) {
            call.transformTo(child);
            return;
        }

        // Drop a no-fetch outer Sort when an inner OpenSearchSort with fetch already produces
        // the same ordering through a Project chain. With both Sorts present, DataFusion's
        // logical-plan optimizer eliminates the inner Sort as redundant but leaves the Limit,
        // then physical-planning pushes Limit down through CoalescePartitionsExec — so fetch
        // is applied BEFORE sort against the unsorted Aggregate output.
        if (sort.fetch == null && sort.offset == null && !sort.getCollation().getFieldCollations().isEmpty()) {
            OpenSearchSort inner = findInnerSortWithFetchThroughProjects(child);
            if (inner != null && outerCollationMatchesInner(sort, child, inner)) {
                call.transformTo(child);
                return;
            }
        }

        // Push the outer pure-fetch's fetch down into a collated inner Sort. Pattern:
        // Sort(no-coll, fetch=K) ← (projects) ← Sort(coll, no-fetch) ← child
        // — typically the implicit system query-size LIMIT wrapped around `ORDER BY x`
        // (no explicit user LIMIT). Pushing the fetch down lets OpenSearchSortSplitRule's
        // PARTIAL+FINAL alternative fire with a bounded shard fetch. The system-injected
        // fetch is silently clamped to the per-index max_result_window — user-explicit
        // limits go through validateMaxResultWindow below and hard-error when too large.
        if (sort.getCollation().getFieldCollations().isEmpty() && sort.offset == null && sort.fetch != null) {
            OpenSearchSort innerCollated = findInnerCollatedSortWithoutFetchThroughProjects(child);
            if (innerCollated != null) {
                RexNode pushedFetch = clampSystemLimitToMaxResultWindow(sort.fetch, innerCollated);
                OpenSearchSort innerWithFetch = (OpenSearchSort) innerCollated.copy(
                    innerCollated.getTraitSet(),
                    innerCollated.getInput(),
                    innerCollated.getCollation(),
                    null,
                    pushedFetch
                );
                RelNode newChild = replaceSortInProjectChain(child, innerCollated, innerWithFetch);
                call.transformTo(newChild);
                return;
            }
        }

        // Reject user-explicit limits exceeding per-index max_result_window.
        if (!sort.getCollation().getFieldCollations().isEmpty() && sort.fetch != null) {
            validateMaxResultWindow(sort, child);
        }

        List<String> childViableBackends = openSearchChild.getViableBackends();
        List<String> sortCapable = context.getCapabilityRegistry().operatorBackends(EngineCapability.SORT);

        List<String> viableBackends = childViableBackends.stream().filter(sortCapable::contains).toList();

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports SORT capability among " + childViableBackends);
        }

        // Calcite's Sort constructor asserts the trait set contains the collation;
        // plus() appends or overrides while replace() is a no-op when the slot is missing.
        call.transformTo(
            new OpenSearchSort(
                sort.getCluster(),
                child.getTraitSet().plus(sort.getCollation()),
                RelNodeUtils.unwrapHep(sort.getInput()),
                sort.getCollation(),
                sort.offset,
                sort.fetch,
                ExecutionMode.SINGLE,
                viableBackends
            )
        );
    }

    private static boolean hasAggregateUnderProjects(RelNode node) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        while (current instanceof OpenSearchProject project) {
            current = RelNodeUtils.unwrapHep(project.getInput());
        }
        return current instanceof OpenSearchAggregate;
    }

    /** Walks down through OpenSearchProjects, returns the first OpenSearchSort with fetch != null. */
    private static OpenSearchSort findInnerSortWithFetchThroughProjects(RelNode node) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        while (current instanceof OpenSearchProject project) {
            current = RelNodeUtils.unwrapHep(project.getInput());
        }
        return current instanceof OpenSearchSort innerSort && innerSort.fetch != null ? innerSort : null;
    }

    /**
     * Walks down through OpenSearchProjects, returns the first OpenSearchSort that has
     * collation but no fetch and no offset — the canonical "user ORDER BY without LIMIT"
     * shape. Used by the system-LIMIT push-down rewrite.
     */
    private static OpenSearchSort findInnerCollatedSortWithoutFetchThroughProjects(RelNode node) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        while (current instanceof OpenSearchProject project) {
            current = RelNodeUtils.unwrapHep(project.getInput());
        }
        if (current instanceof OpenSearchSort innerSort
            && innerSort.fetch == null
            && innerSort.offset == null
            && innerSort.getCollation().getFieldCollations().isEmpty() == false) {
            return innerSort;
        }
        return null;
    }

    /**
     * Walks down through OpenSearchProjects, replacing {@code target} with {@code replacement}
     * in the chain. Returns the new chain top. Throws if the walk encounters anything other
     * than OpenSearchProjects on the way down.
     */
    private static RelNode replaceSortInProjectChain(RelNode chainTop, OpenSearchSort target, OpenSearchSort replacement) {
        RelNode current = RelNodeUtils.unwrapHep(chainTop);
        if (current == target) return replacement;
        if (current instanceof OpenSearchProject project) {
            RelNode rebuilt = replaceSortInProjectChain(project.getInput(), target, replacement);
            return project.copy(project.getTraitSet(), List.of(rebuilt));
        }
        throw new IllegalStateException(
            "replaceSortInProjectChain: unexpected node "
                + current.getClass().getSimpleName()
                + " (expected OpenSearchProject or target Sort)"
        );
    }

    // ── max_result_window cap ──────────────────────────────────────────────────

    /**
     * Rejects user-explicit limits where {@code offset + fetch} exceeds the per-index
     * {@code index.max_result_window}, matching native OpenSearch's "Result window is
     * too large" behavior. Multi-table Sorts (Join, Union) take the {@code min} across
     * involved indices.
     */
    private void validateMaxResultWindow(Sort sort, RelNode subtree) {
        if (!(sort.fetch instanceof RexLiteral fetchLit)) return;
        long offset = sort.offset instanceof RexLiteral offLit ? RexLiteral.intValue(offLit) : 0L;
        long fetch = RexLiteral.intValue(fetchLit);
        long bound;
        try {
            bound = Math.addExact(offset, fetch);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                "Result window is too large, [from + size] overflows int. Use a smaller LIMIT/OFFSET, or paginate."
            );
        }
        int cap = resolveMinMaxResultWindow(subtree);
        if (bound > cap) {
            throw new IllegalArgumentException(
                "Result window is too large, [from + size] = "
                    + bound
                    + " must be less than or equal to: ["
                    + cap
                    + "]. Use a smaller LIMIT, or override [index.max_result_window] per-index."
            );
        }
    }

    /**
     * Clamps a system-injected fetch (frontend safety net) to the per-index cap.
     * Returns the original RexNode unchanged when no clamp is needed.
     */
    private RexNode clampSystemLimitToMaxResultWindow(RexNode systemFetch, RelNode subtree) {
        if (!(systemFetch instanceof RexLiteral fetchLit)) return systemFetch;
        long fetch = RexLiteral.intValue(fetchLit);
        int cap = resolveMinMaxResultWindow(subtree);
        if (fetch <= cap) return systemFetch;
        RexBuilder rexBuilder = subtree.getCluster().getRexBuilder();
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(cap), systemFetch.getType());
    }

    /**
     * Minimum {@code index.max_result_window} across every {@link OpenSearchTableScan}
     * in {@code subtree}. Falls back to the setting's default when no scans are present
     * (e.g. Sort over LogicalValues).
     */
    private int resolveMinMaxResultWindow(RelNode subtree) {
        int defaultWindow = IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(org.opensearch.common.settings.Settings.EMPTY);
        int[] min = { Integer.MAX_VALUE };
        boolean[] any = { false };
        collectMinWindow(subtree, min, any);
        return any[0] ? min[0] : defaultWindow;
    }

    private void collectMinWindow(RelNode node, int[] min, boolean[] any) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        if (current instanceof OpenSearchTableScan scan) {
            int window = readMaxResultWindow(scan);
            if (window < min[0]) min[0] = window;
            any[0] = true;
            return;
        }
        for (RelNode input : current.getInputs()) {
            collectMinWindow(input, min, any);
        }
    }

    private int readMaxResultWindow(RelNode scan) {
        String tableName = scan.getTable().getQualifiedName().getLast();
        IndexMetadata indexMetadata = context.getClusterState().metadata().index(tableName);
        // OpenSearchTableScanRule rejects unknown indices before this rule fires, so a
        // missing IndexMetadata or null settings here is a planner bug.
        assert indexMetadata != null : "IndexMetadata missing for [" + tableName + "]";
        assert indexMetadata.getSettings() != null : "IndexMetadata.getSettings() null for [" + tableName + "]";
        return IndexSettings.MAX_RESULT_WINDOW_SETTING.get(indexMetadata.getSettings());
    }

    /**
     * Checks that the outer Sort's collation, when its field references are remapped down
     * through each intermediate OpenSearchProject's identity-projection exprs, matches the
     * inner Sort's collation field-for-field.
     */
    private static boolean outerCollationMatchesInner(Sort outer, RelNode child, OpenSearchSort inner) {
        List<RelFieldCollation> outerFields = outer.getCollation().getFieldCollations();
        List<RelFieldCollation> innerFields = inner.getCollation().getFieldCollations();
        if (outerFields.size() != innerFields.size()) {
            return false;
        }
        for (int i = 0; i < outerFields.size(); i++) {
            RelFieldCollation outerField = outerFields.get(i);
            int remapped = remapInputIndexThroughProjects(outerField.getFieldIndex(), child, inner);
            if (remapped < 0) {
                return false;
            }
            RelFieldCollation innerField = innerFields.get(i);
            if (remapped != innerField.getFieldIndex()
                || outerField.getDirection() != innerField.getDirection()
                || outerField.nullDirection != innerField.nullDirection) {
                return false;
            }
        }
        return true;
    }

    /**
     * Walks `node` down to `inner`, translating `index` through each OpenSearchProject's
     * exprs. Each project's expr at position `i` must be a RexInputRef for the index to
     * remap; non-identity exprs return -1 (unmappable).
     */
    private static int remapInputIndexThroughProjects(int index, RelNode node, OpenSearchSort inner) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        int idx = index;
        while (current instanceof OpenSearchProject project) {
            if (idx < 0 || idx >= project.getProjects().size()) {
                return -1;
            }
            RexNode expr = project.getProjects().get(idx);
            if (!(expr instanceof RexInputRef ref)) {
                return -1;
            }
            idx = ref.getIndex();
            current = RelNodeUtils.unwrapHep(project.getInput());
        }
        return current == inner ? idx : -1;
    }
}

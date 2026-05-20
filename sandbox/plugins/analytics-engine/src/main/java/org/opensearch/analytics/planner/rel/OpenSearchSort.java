/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * OpenSearch custom Sort carrying viable backend list and a distributed
 * {@link ExecutionMode}.
 *
 * <ul>
 *   <li>{@code SINGLE} — full sort in one pass on a single node. Always emitted by
 *       marking; Volcano either keeps it or splits via
 *       {@link org.opensearch.analytics.planner.rules.OpenSearchSortSplitRule}.</li>
 *   <li>{@code PARTIAL} — shard-local top-K with {@code fetch = offset + fetch}
 *       and {@code offset = null}.</li>
 *   <li>{@code FINAL} — coordinator-side merge over gathered partial outputs,
 *       carrying the original {@code offset} and {@code fetch}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class OpenSearchSort extends Sort implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final ExecutionMode mode;

    public OpenSearchSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch,
        ExecutionMode mode,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input, collation, offset, fetch);
        this.mode = mode;
        this.viableBackends = viableBackends;
    }

    public ExecutionMode getMode() {
        return mode;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Sort doesn't change schema — pass through child's field storage. */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new OpenSearchSort(getCluster(), traitSet, input, collation, offset, fetch, mode, viableBackends);
    }

    /**
     * Treat our Sort as a concrete physical operator, not a Calcite collation enforcer.
     *
     * <p>Calcite's default classifies a Sort with collation as an enforcer — Volcano then
     * registers it into a {@code required=true} subset that's never marked delivered. That
     * confuses the gather-rule path, which looks for delivered subsets when converting an
     * inner Sort's RelSet to SINGLETON. We don't use Calcite's collation-trait enforcement,
     * so mark the Sort delivered like any other operator.
     */
    @Override
    public boolean isEnforcer() {
        return false;
    }

    /**
     * Cost gate: only {@code SINGLE} with non-empty collation over a non-SINGLETON
     * input pays infinite cost — forces Volcano to pick the
     * {@link org.opensearch.analytics.planner.rules.OpenSearchSortSplitRule}
     * alternative. {@code PARTIAL} is shard-local by contract; {@code FINAL} sits
     * over an ER (SINGLETON input by construction); pure-LIMIT {@code SINGLE} (empty
     * collation) is partition-local-correct. All three are tiny.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (mode != ExecutionMode.SINGLE) {
            return planner.getCostFactory().makeTinyCost();
        }
        if (getCollation().getFieldCollations().isEmpty()) {
            return planner.getCostFactory().makeTinyCost();
        }
        for (RelNode input : getInputs()) {
            for (int i = 0; i < input.getTraitSet().size(); i++) {
                RelTrait trait = input.getTraitSet().getTrait(i);
                if (trait instanceof OpenSearchDistribution distribution) {
                    boolean singletonOrAny = distribution.getType() == RelDistribution.Type.SINGLETON
                        || distribution.getType() == RelDistribution.Type.ANY;
                    if (!singletonOrAny) {
                        return planner.getCostFactory().makeInfiniteCost();
                    }
                }
            }
        }
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("mode", mode).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchSort(getCluster(), getTraitSet(), children.getFirst(), getCollation(), offset, fetch, mode, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // LogicalSort has no execution-mode concept; backend convertor only sees a logical Sort.
        return LogicalSort.create(strippedChildren.getFirst(), getCollation(), offset, fetch);
    }
}

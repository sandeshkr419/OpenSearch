/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchSort}.
 *
 * <p>Two flavors:
 * <ul>
 *   <li><b>Collated Sort</b> (ORDER BY) — needs SINGLETON input. Volcano's
 *       {@code OpenSearchSortSplitRule} fires {@code convert(input, COORDINATOR)} which
 *       inserts an ER under the Sort. Sort runs at coord.</li>
 *   <li><b>Pure-LIMIT Sort</b> (no collation, just {@code fetch}) — partition-local
 *       fetch is correct. ER goes above the Sort to gather to coord.</li>
 * </ul>
 */
public class SortPlanShapeTests extends PlanShapeTestBase {

    /**
     * 1-shard collated Sort: SHARD+SINGLETON satisfies the root SINGLETON demand, so
     * no ER is inserted and the Sort sits directly on the scan.
     */
    public void testCollatedSort_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeSort(scan, /* fetch */ -1);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchSort(sort0=[$0], dir0=[ASC], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testCollatedSort_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeSort(scan, /* fetch */ -1);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testPureLimit_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeLimit(scan, 10);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchSort(fetch=[10], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testPureLimit_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeLimit(scan, 10);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchSort(fetch=[10], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * 1-shard sort + LIMIT — fetch is pushed into the inner collated Sort, and
     * SHARD+SINGLETON satisfies the demand directly, so the Sort sits at the shard.
     */
    public void testSortPlusLimit_1shard() {
        RelNode result = runPlanner(buildSortPlusLimit(), singleShardContext());
        assertPlanShape("""
            OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testSortPlusLimit_2shard() {
        RelNode result = runPlanner(buildSortPlusLimit(), multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** PPL frontend emits two LogicalSort nodes for {@code | sort x | head 10}: an outer
     *  pure-fetch and an inner collated. Constructed by hand here for the same shape. */
    private RelNode buildSortPlusLimit() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode innerSort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        return makeLimit(innerSort, 10);
    }

    /**
     * Multi-shard top-K: ORDER BY $0 LIMIT 10 over a multi-shard table. The inner Sort
     * has both collation and fetch, so the split rule's Alternative B fires:
     *   FINAL Sort(coll, fetch=10) ← ER ← PARTIAL Sort(coll, fetch=10) ← scan
     * Coordinator memory is bounded by N_shards × 10 instead of all matching rows.
     */
    public void testTopKSort_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeSort(scan, /* fetch */ 10);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Single-shard top-K: child is SHARD+SINGLETON which already satisfies the
     * SINGLE-on-SINGLETON alternative without an ER. Volcano picks Alternative A
     * (cheaper than PARTIAL+ER+FINAL with one shard); Sort sits directly on the scan.
     */
    public void testTopKSort_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeSort(scan, /* fetch */ 10);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * Multi-shard top-K with non-zero OFFSET: shards must NOT apply offset. Each shard
     * pushes fetch = offset + limit; coordinator applies the original offset, fetch.
     */
    public void testTopKSort_2shard_withOffset() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        // ORDER BY $0 ASC LIMIT 10 OFFSET 5 — partialFetch should be 15 on each shard.
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], offset=[5], fetch=[10], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[15], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Top-K over a multi-shard Aggregate. The aggregate's FINAL output is already
     * COORDINATOR+SINGLETON, so the Sort's SINGLE-on-SINGLETON alternative is satisfied
     * without inserting another ER above. Volcano picks Alternative A; no PARTIAL+FINAL
     * sort split (it would only re-bound rows already bounded by group cardinality).
     */
    public void testTopKSortOverAggregate_2shard() {
        RelNode agg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode plan = makeSort(agg, /* fetch */ 10);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testCollatedSort_2shard_descending() {
        // sort -status — DESC NULLS LAST is Calcite's default for DESC.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[DESC], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ── max_result_window cap ───────────────────────────────────────────────

    /** offset + fetch == default cap (10000) succeeds — query plan is produced without error. */
    public void testMaxResultWindow_atBoundary() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10000, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true)
        );
        // Just asserting no exception — exact plan shape (Alt A vs Alt B) depends on
        // Volcano's cost decision against mocked row counts, which isn't what this test is
        // about. The cap-related shape tests are above; here we only verify the cap allows
        // queries up to its limit.
        RelNode result = runPlanner(plan, multiShardContext());
        assertNotNull("planner must produce a plan at the cap boundary", result);
    }

    /** offset + fetch > cap is rejected with a native-OS-style error message. */
    public void testMaxResultWindow_aboveBoundary_rejected() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10001, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true)
        );
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> runPlanner(plan, multiShardContext()));
        assertTrue(
            "error message must mention the bound and the cap, got: " + ex.getMessage(),
            ex.getMessage().contains("[from + size] = 10001") && ex.getMessage().contains("[10000]")
        );
        assertTrue(
            "error message must point users at the override knob, got: " + ex.getMessage(),
            ex.getMessage().contains("index.max_result_window")
        );
    }

    /** offset > 0 case — offset+fetch must be ≤ cap, not just fetch. */
    public void testMaxResultWindow_offsetCounts() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            rexBuilder.makeLiteral(9990, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(100, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true)
        );
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> runPlanner(plan, multiShardContext()));
        assertTrue("error must reflect from+size = 10090, got: " + ex.getMessage(), ex.getMessage().contains("[from + size] = 10090"));
    }

    /**
     * System-injected limit (outer pure-fetch over inner collated) with fetch=50000 is
     * silently clamped to the per-index cap (10000). The user didn't ask for 50000 —
     * the frontend's safety net did — so erroring would be wrong UX; clamping preserves
     * correctness while bounding memory.
     */
    public void testSystemLimit_clampsToCap() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode innerSort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode systemLimit = LogicalSort.create(
            innerSort,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(50000, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(systemLimit, multiShardContext());
        // The exact split shape (Alt A vs Alt B) depends on Volcano's cost, which against
        // mocked row counts isn't what we want to lock. What matters is the clamp happened:
        // somewhere in the produced plan there's a Sort with fetch=[10000] (the cap), and
        // NO Sort with fetch=[50000] (the unclamped system value).
        String planStr = org.apache.calcite.plan.RelOptUtil.toString(result);
        assertTrue("plan must contain Sort with clamped fetch=[10000], got:\n" + planStr, planStr.contains("fetch=[10000]"));
        assertFalse("plan must NOT contain unclamped fetch=[50000], got:\n" + planStr, planStr.contains("fetch=[50000]"));
    }
}

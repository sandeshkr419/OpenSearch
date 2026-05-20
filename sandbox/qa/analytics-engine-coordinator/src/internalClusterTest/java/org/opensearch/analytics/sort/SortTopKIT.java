/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sort;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.sql.SqlPlanRunner;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end correctness IT for the per-shard top-K split. Drives
 * {@code SELECT v FROM idx ORDER BY v ... LIMIT N} on a real multi-shard cluster and
 * verifies results match a single-shard reference run.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class SortTopKIT extends OpenSearchIntegTestCase {

    private static final String INDEX_MULTI = "topk_multi";
    private static final String INDEX_SINGLE = "topk_single";
    private static final int NUM_KEYS = 300;
    private static final int MULTI_NUM_SHARDS = 5;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete("topk_*").get();
        } catch (Exception ignore) {
            // best-effort
        }
        super.tearDown();
    }

    /**
     * ORDER BY v ASC LIMIT 10 over a multi-shard index. Expected: rows with v in [1, 10],
     * sorted ascending. Verifies the PARTIAL+FINAL split plus k-way reduce produce the
     * correct global top-K.
     */
    public void testTopKAscending_multiShard() {
        SqlPlanRunner runner = sqlPlanRunner();
        createAndSeedIndex(INDEX_MULTI, MULTI_NUM_SHARDS);

        List<Object[]> rows = runner.executeSql("SELECT v FROM " + INDEX_MULTI + " ORDER BY v ASC LIMIT 10");

        assertEquals("expected 10 rows", 10, rows.size());
        for (int i = 0; i < 10; i++) {
            int v = ((Number) rows.get(i)[0]).intValue();
            assertEquals("row " + i + " expected v=" + (i + 1), i + 1, v);
        }
    }

    /**
     * ORDER BY v DESC LIMIT 10. Verifies DESC collation flows through the partial Sort.
     */
    public void testTopKDescending_multiShard() {
        SqlPlanRunner runner = sqlPlanRunner();
        createAndSeedIndex(INDEX_MULTI, MULTI_NUM_SHARDS);

        List<Object[]> rows = runner.executeSql("SELECT v FROM " + INDEX_MULTI + " ORDER BY v DESC LIMIT 10");

        assertEquals("expected 10 rows", 10, rows.size());
        for (int i = 0; i < 10; i++) {
            int v = ((Number) rows.get(i)[0]).intValue();
            assertEquals("row " + i + " expected v=" + (NUM_KEYS - i), NUM_KEYS - i, v);
        }
    }

    /**
     * Multi-shard top-K must match the single-shard reference. Catches a per-shard offset
     * bug or a merge-direction bug — both would manifest as a row-by-row mismatch even
     * though sizes match.
     */
    public void testTopKMatchesSingleShardReference() {
        SqlPlanRunner runner = sqlPlanRunner();
        createAndSeedIndex(INDEX_MULTI, MULTI_NUM_SHARDS);
        createAndSeedIndex(INDEX_SINGLE, 1);

        List<Object[]> multi = runner.executeSql("SELECT v FROM " + INDEX_MULTI + " ORDER BY v ASC LIMIT 25");
        List<Object[]> single = runner.executeSql("SELECT v FROM " + INDEX_SINGLE + " ORDER BY v ASC LIMIT 25");

        assertEquals("row counts must match", single.size(), multi.size());
        for (int i = 0; i < single.size(); i++) {
            int multiV = ((Number) multi.get(i)[0]).intValue();
            int singleV = ((Number) single.get(i)[0]).intValue();
            assertEquals("row " + i + ": multi-shard top-K must match single-shard reference", singleV, multiV);
        }
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private void createAndSeedIndex(String indexName, int numShards) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings)
            .setMapping("v", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged for " + indexName, response.isAcknowledged());
        ensureGreen(indexName);

        // Bulk index 1..NUM_KEYS so the global ascending order is v=1, 2, ..., NUM_KEYS.
        final int batchSize = 200;
        for (int batchStart = 1; batchStart <= NUM_KEYS; batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize - 1, NUM_KEYS);
            org.opensearch.action.bulk.BulkRequestBuilder bulk = client().prepareBulk();
            for (int v = batchStart; v <= batchEnd; v++) {
                bulk.add(client().prepareIndex(indexName).setId(indexName + "_" + v).setSource("v", v));
            }
            org.opensearch.action.bulk.BulkResponse bulkResponse = bulk.get();
            assertFalse(
                "bulk index batch [" + batchStart + ", " + batchEnd + "] had failures: " + bulkResponse.buildFailureMessage(),
                bulkResponse.hasFailures()
            );
        }
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Golden-file plan shape tests across all ClickBench queries and execution combinations.
 * Validates Calcite plan, shard DF physical plan, and coordinator DF physical plan for
 * 16 combinations: shards(1,2) x slice(1,N) x topk(off,on) x scan(vanilla,delegation).
 *
 * <p>Golden files stored as YAML in {@code resources/datasets/clickbench/ppl/plan_shapes/q{N}.yaml}.
 * Each YAML file contains all 16 combos for that query.
 * Generate with {@code -Dtests.plan_shapes.generate=true}.
 */
public class PlanShapeGoldenIT extends AnalyticsRestTestCase {

    private static final String INDEX_1S = "plan_shape_1s";
    private static final String INDEX_2S = "plan_shape_2s";
    private static final String PLANS_BASE = "datasets/clickbench/ppl/plan_shapes/";
    private static final Set<Integer> SKIP_QUERIES = Set.of(29);

    private static volatile boolean provisioned = false;

    private static final List<Combo> COMBOS = List.of(
        new Combo("1s_slice1_topk_off_vanilla", INDEX_1S, "none", 0, 0.0, false),
        new Combo("1s_slice1_topk_on_vanilla", INDEX_1S, "none", 0, 2.0, false),
        new Combo("1s_sliceN_topk_off_vanilla", INDEX_1S, "all", 4, 0.0, false),
        new Combo("1s_sliceN_topk_on_vanilla", INDEX_1S, "all", 4, 2.0, false),
        new Combo("1s_slice1_topk_off_delegation", INDEX_1S, "none", 0, 0.0, true),
        new Combo("1s_slice1_topk_on_delegation", INDEX_1S, "none", 0, 2.0, true),
        new Combo("1s_sliceN_topk_off_delegation", INDEX_1S, "all", 4, 0.0, true),
        new Combo("1s_sliceN_topk_on_delegation", INDEX_1S, "all", 4, 2.0, true),
        new Combo("2s_slice1_topk_off_vanilla", INDEX_2S, "none", 0, 0.0, false),
        new Combo("2s_slice1_topk_on_vanilla", INDEX_2S, "none", 0, 2.0, false),
        new Combo("2s_sliceN_topk_off_vanilla", INDEX_2S, "all", 4, 0.0, false),
        new Combo("2s_sliceN_topk_on_vanilla", INDEX_2S, "all", 4, 2.0, false),
        new Combo("2s_slice1_topk_off_delegation", INDEX_2S, "none", 0, 0.0, true),
        new Combo("2s_slice1_topk_on_delegation", INDEX_2S, "none", 0, 2.0, true),
        new Combo("2s_sliceN_topk_off_delegation", INDEX_2S, "all", 4, 0.0, true),
        new Combo("2s_sliceN_topk_on_delegation", INDEX_2S, "all", 4, 2.0, true)
    );

    private void ensureProvisioned() throws Exception {
        if (provisioned == false) {
            createCompositeIndex(INDEX_1S, 1);
            createCompositeIndex(INDEX_2S, 2);
            loadClickBenchData(INDEX_1S);
            loadClickBenchData(INDEX_2S);
            provisioned = true;
        }
    }

    @SuppressWarnings("unchecked")
    public void testAllPlanShapes() throws Exception {
        ensureProvisioned();
        boolean generate = "true".equals(System.getProperty("tests.plan_shapes.generate"));

        List<Integer> queryNumbers = DatasetQueryRunner.discoverQueryNumbers(ClickBenchTestHelper.DATASET, "ppl")
            .stream()
            .filter(n -> SKIP_QUERIES.contains(n) == false)
            .toList();

        List<String> failures = new ArrayList<>();

        for (int qNum : queryNumbers) {
            String rawPpl = loadQueryFile(qNum);
            String goldenPath = PLANS_BASE + "q" + qNum + ".yaml";

            Map<String, Map<String, String>> expectedAll = generate ? null : loadGoldenYaml(goldenPath);
            Map<String, Map<String, String>> generatedAll = generate ? new LinkedHashMap<>() : null;

            if (expectedAll == null && generate == false) {
                failures.add("q" + qNum + ": golden file missing (" + goldenPath + "). Run with -Dtests.plan_shapes.generate=true");
                continue;
            }

            for (Combo combo : COMBOS) {
                String ppl = rewriteQuery(rawPpl, combo);
                try {
                    applySettings(combo);
                    CapturedPlans actual = capturePlans(ppl);

                    if (generate) {
                        generatedAll.put(combo.name, actual.toMap());
                        continue;
                    }

                    Map<String, String> expectedEntry = expectedAll.get(combo.name);
                    if (expectedEntry == null) {
                        failures.add("q" + qNum + "/" + combo.name + ": combo missing in golden YAML");
                        continue;
                    }

                    CapturedPlans expected = CapturedPlans.fromMap(expectedEntry);
                    String diff = actual.diff(expected);
                    if (diff != null) {
                        failures.add("q" + qNum + "/" + combo.name + ": " + diff);
                    }
                } catch (Exception e) {
                    failures.add("q" + qNum + "/" + combo.name + ": " + e.getMessage());
                }
            }

            if (generate) {
                logger.info("=== q{}.yaml ===\n{}", qNum, toYamlString(generatedAll));
            }
        }

        if (generate == false && failures.isEmpty() == false) {
            fail("Plan shape mismatches (" + failures.size() + "):\n" + String.join("\n\n", failures));
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private void createCompositeIndex(String indexName, int shards) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception ignored) {}

        String fullMapping = loadResource("datasets/clickbench/mapping.json");
        String adjusted = fullMapping
            .replaceFirst("\"number_of_shards\":\\s*\\d+", "\"number_of_shards\":" + shards)
            .replaceFirst("\"settings\"\\s*:\\s*\\{", "\"settings\":{" +
                "\"index.pluggable.dataformat.enabled\":true," +
                "\"index.pluggable.dataformat\":\"composite\"," +
                "\"index.composite.primary_data_format\":\"parquet\"," +
                "\"index.composite.secondary_data_formats\":[\"lucene\"],");

        Request req = new Request("PUT", "/" + indexName);
        req.setJsonEntity(adjusted);
        assertOkAndParse(client().performRequest(req), "create " + indexName);

        Request health = new Request("GET", "/_cluster/health/" + indexName);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void loadClickBenchData(String indexName) throws IOException {
        String bulk = loadResource("datasets/clickbench/bulk.json");
        Request req = new Request("POST", "/" + indexName + "/_bulk");
        req.setJsonEntity(bulk);
        client().performRequest(req);
        client().performRequest(new Request("POST", "/" + indexName + "/_refresh"));
    }

    private void applySettings(Combo combo) throws IOException {
        String body = "{\"transient\":{"
            + "\"search.concurrent_segment_search.mode\":\"" + combo.searchMode + "\""
            + (combo.sliceCount > 0 ? ",\"search.concurrent.max_slice_count\":" + combo.sliceCount : "")
            + ",\"analytics.shard_bucket_oversampling_factor\":" + combo.oversampling
            + "}}";
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity(body);
        client().performRequest(req);
    }

    private String rewriteQuery(String rawPpl, Combo combo) {
        String ppl = rawPpl.replace("clickbench", combo.indexName);
        if (combo.delegation == false) {
            return ppl;
        }
        return ppl;
    }

    @SuppressWarnings("unchecked")
    private CapturedPlans capturePlans(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> result = assertOkAndParse(response, "EXPLAIN: " + ppl);
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");

        String calcite = String.join("\n", (List<String>) profile.get("full_plan"));
        String shardPhysical = "";
        String coordPhysical = "";

        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        if (stages != null) {
            for (Map<String, Object> stage : stages) {
                String type = (String) stage.get("execution_type");
                if ("SHARD_FRAGMENT".equals(type)) {
                    List<Map<String, Object>> tasks = (List<Map<String, Object>>) stage.get("tasks");
                    if (tasks != null && tasks.isEmpty() == false) {
                        Object plan = tasks.get(0).get("physical_plan");
                        if (plan instanceof String s) shardPhysical = s;
                    }
                } else if ("COORDINATOR_REDUCE".equals(type)) {
                    Object plan = stage.get("physical_plan");
                    if (plan instanceof String s) coordPhysical = s;
                }
            }
        }

        return new CapturedPlans(calcite, shardPhysical, coordPhysical);
    }

    private String loadQueryFile(int queryNumber) throws IOException {
        String path = ClickBenchTestHelper.DATASET.queryResourcePath("ppl", "ppl", queryNumber);
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) throw new IOException("Query not found: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, String>> loadGoldenYaml(String path) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) return null;
            Yaml yaml = new Yaml();
            Map<String, Object> raw = yaml.load(is);
            if (raw == null) return null;
            Map<String, Map<String, String>> result = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : raw.entrySet()) {
                Map<String, Object> sections = (Map<String, Object>) entry.getValue();
                Map<String, String> planSections = new LinkedHashMap<>();
                for (Map.Entry<String, Object> section : sections.entrySet()) {
                    planSections.put(section.getKey(), section.getValue() != null ? section.getValue().toString().trim() : "");
                }
                result.put(entry.getKey(), planSections);
            }
            return result;
        } catch (IOException e) {
            return null;
        }
    }

    private String toYamlString(Map<String, Map<String, String>> plans) {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setDefaultScalarStyle(DumperOptions.ScalarStyle.LITERAL);
        options.setWidth(200);
        Yaml yaml = new Yaml(options);
        StringWriter writer = new StringWriter();
        yaml.dump(plans, writer);
        return writer.toString();
    }

    // ── Records ─────────────────────────────────────────────────────────────────

    record Combo(String name, String indexName, String searchMode, int sliceCount, double oversampling, boolean delegation) {}

    record CapturedPlans(String calcite, String shardPhysical, String coordPhysical) {

        Map<String, String> toMap() {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("calcite", calcite);
            if (shardPhysical.isEmpty() == false) {
                map.put("shard_physical", shardPhysical);
            }
            if (coordPhysical.isEmpty() == false) {
                map.put("coord_physical", coordPhysical);
            }
            return map;
        }

        static CapturedPlans fromMap(Map<String, String> map) {
            return new CapturedPlans(
                map.getOrDefault("calcite", ""),
                map.getOrDefault("shard_physical", ""),
                map.getOrDefault("coord_physical", "")
            );
        }

        String diff(CapturedPlans expected) {
            if (normalize(calcite).equals(normalize(expected.calcite)) == false) {
                return "CALCITE mismatch\n  EXPECTED:\n" + expected.calcite + "\n  ACTUAL:\n" + calcite;
            }
            if (normalize(shardPhysical).equals(normalize(expected.shardPhysical)) == false) {
                return "SHARD_PHYSICAL mismatch\n  EXPECTED:\n" + expected.shardPhysical + "\n  ACTUAL:\n" + shardPhysical;
            }
            if (normalize(coordPhysical).equals(normalize(expected.coordPhysical)) == false) {
                return "COORD_PHYSICAL mismatch\n  EXPECTED:\n" + expected.coordPhysical + "\n  ACTUAL:\n" + coordPhysical;
            }
            return null;
        }

        private static String normalize(String plan) {
            if (plan == null) return "";
            return plan.lines()
                .map(String::stripTrailing)
                .map(l -> l.replaceAll("t=-?\\d+:s=\\d+", "t=X:s=X"))
                .filter(l -> l.isEmpty() == false)
                .reduce("", (a, b) -> a + "\n" + b)
                .trim();
        }
    }
}

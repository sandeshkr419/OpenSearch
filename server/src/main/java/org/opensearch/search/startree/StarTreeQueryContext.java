/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Query class for querying star tree data structure.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeQueryContext {

    /**
     * Star tree field info
     * This is used to get the star tree data structure
     */
    private final CompositeIndexFieldInfo starTree;

    /**
     * Map of field name to a value to be queried for that field
     * This is used to filter the data based on the query
     */
    private final Map<String, Long> queryMap;

//    /**
//     * Cache for leaf results
//     * This is used to cache the results for each leaf reader context
//     * to avoid reading the data from the leaf reader context multiple times
//     */
//    private volatile Map<LeafReaderContext, Map<String, StarTreeQueryHelper.MetricInfo>> leafResultsCache;

//    /**
//     * List of metrics to be computed & cached
//     */
//    private List<StarTreeQueryHelper.MetricInfo> metrics;

    public StarTreeQueryContext(CompositeIndexFieldInfo starTree, Map<String, Long> queryMap) {
        this.starTree = starTree;
        this.queryMap = queryMap;
    }

    public CompositeIndexFieldInfo getStarTree() {
        return starTree;
    }

    public Map<String, Long> getQueryMap() {
        return queryMap;
    }

//    public void initializeLeafResultsCache() {
//        this.leafResultsCache = new ConcurrentHashMap<>();
//        this.metrics = new ArrayList<>();
//    }
//
//    public Map<LeafReaderContext, Map<String, StarTreeQueryHelper.MetricInfo>> getLeafResultsCache() {
//        return leafResultsCache;
//    }
//
//    public void addMetric(StarTreeQueryHelper.MetricInfo metric) {
//        metrics.add(metric);
//    }

}

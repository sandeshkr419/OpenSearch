/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;

import java.util.Map;

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
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StarTreeAggregatorFactory extends AggregatorFactory {
    private List<String> fieldCols;
    private List<String> metrics;
    String subAggregationName;

    public StarTreeAggregatorFactory(
        String aggregationName,
        String subAggregationName,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        List<String> fieldCols,
        List<String> metrics
    ) throws IOException {
        super(aggregationName, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.fieldCols = fieldCols;
        this.metrics = metrics;
        this.subAggregationName = subAggregationName;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return new StarTreeAggregator(name, subAggregationName, factories, searchContext, parent, metadata, fieldCols, metrics);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}

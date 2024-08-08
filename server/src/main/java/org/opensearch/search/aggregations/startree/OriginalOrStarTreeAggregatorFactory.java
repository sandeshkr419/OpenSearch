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
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OriginalOrStarTreeAggregatorFactory extends AggregatorFactory {

    List<StarTreeAggregatorFactory> starTreeAggregatorFactories;
    TermsAggregatorFactory originalAggregatorFactory;

    public OriginalOrStarTreeAggregatorFactory(
        String aggregationName,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        List<StarTreeAggregatorFactory> starTreeAggregatorFactories,
        TermsAggregatorFactory originalAggregatorFactory
    ) throws IOException {
        super(aggregationName, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.starTreeAggregatorFactories = starTreeAggregatorFactories;
        this.originalAggregatorFactory = originalAggregatorFactory;
    }

    @Override
    protected Aggregator createInternal(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
        TermsAggregator originalAggregator = (TermsAggregator) this.originalAggregatorFactory.createInternal(searchContext, parent, cardinality, metadata);
        this.starTreeAggregatorFactories.get(0).setOriginalAggregator(originalAggregator);
        return this.starTreeAggregatorFactories.get(0).createInternal(searchContext, parent, cardinality, metadata);
    }
}

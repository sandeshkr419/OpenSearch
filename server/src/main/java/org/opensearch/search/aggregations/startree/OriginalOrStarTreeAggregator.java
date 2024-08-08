/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregator;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class OriginalOrStarTreeAggregator extends BucketsAggregator implements SingleBucketAggregator {


    public OriginalOrStarTreeAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent, CardinalityUpperBound bucketCardinality, Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, bucketCardinality, metadata);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return null;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return new InternalAggregation[0];
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return null;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregator for HyperLogLog++ fields that can merge pre-computed sketches
 * during rollup operations.
 *
 * @opensearch.internal
 */
public class HyperLogLogAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Bytes valuesSource;
    private byte[] mergedSketch;
    private int precision;

    public HyperLogLogAggregator(
        String name,
        ValuesSource.Bytes valuesSource,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.precision = 14; // Default precision
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (mergedSketch == null) {
            return 0;
        }
        try {
            return HyperLogLogSerializer.getCardinality(mergedSketch);
        } catch (IOException e) {
            throw new OpenSearchException("Failed to get cardinality from HyperLogLog sketch", e);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (mergedSketch == null) {
            return buildEmptyAggregation();
        }
        try {
            long cardinality = HyperLogLogSerializer.getCardinality(mergedSketch);
            return new InternalCardinality(name, cardinality, metadata());
        } catch (IOException e) {
            throw new OpenSearchException("Failed to build HyperLogLog aggregation", e);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, 0, metadata());
    }

    @Override
    public LeafBucketCollector getLeafCollector(org.apache.lucene.index.LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0; i < valuesCount; i++) {
                        final BytesRef value = values.nextValue();
                        mergeSketch(value.bytes);
                    }
                }
            }
        };
    }

    private void mergeSketch(byte[] sketchBytes) {
        try {
            if (mergedSketch == null) {
                // First sketch, just copy it
                mergedSketch = sketchBytes.clone();
                precision = HyperLogLogSerializer.getPrecision(mergedSketch);
            } else {
                // Merge with existing sketch
                mergedSketch = HyperLogLogSerializer.merge(mergedSketch, sketchBytes);
            }
        } catch (IOException e) {
            throw new OpenSearchException("Failed to merge HyperLogLog sketches", e);
        }
    }

    /**
     * Factory for creating HyperLogLog aggregators
     *
     * @opensearch.internal
     */
    public static class Factory {
        
        public static HyperLogLogAggregator create(
            String name,
            ValuesSource.Bytes valuesSource,
            SearchContext context,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException {
            return new HyperLogLogAggregator(name, valuesSource, context, parent, metadata);
        }
    }
}

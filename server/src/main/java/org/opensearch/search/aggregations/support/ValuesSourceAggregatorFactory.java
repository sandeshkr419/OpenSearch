/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.support;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.Build;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.codec.composite.Composite90DocValuesFormat;
import org.opensearch.index.codec.composite.Composite90DocValuesReader;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.startree.StarTreeAggregator;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for all values source agg factories
 *
 * @opensearch.internal
 */
public abstract class ValuesSourceAggregatorFactory extends AggregatorFactory {

    protected ValuesSourceConfig config;

    public ValuesSourceAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.config = config;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        if (config.hasValues() == false) {
            return createUnmapped(searchContext, parent, metadata);
        }

        boolean starTreeIndexPresent = searchContext.mapperService().isCompositeIndexPresent();
        // Try to see if the query can be resolved using star-tree index
        if (starTreeIndexPresent) {
            try {

                CompositeDataCubeFieldType compositeMappedFieldTypes = (StarTreeMapper.StarTreeFieldType) searchContext.mapperService().getCompositeFieldTypes().iterator().next();

                // TODO: Add support for different query types
//                assert (searchContext.query() instanceof MatchAllDocsQuery);

                // single composite index supported as of now
                // will need to iterate over different composite indices once multiple composite indices are supported
                List<String> supportedFields = new ArrayList<>(compositeMappedFieldTypes.fields());
                List<MetricStat> supportedMetrics = compositeMappedFieldTypes.getMetrics().get(0).getMetrics();

                AggregatorFactories.Builder sourceBuilder = searchContext.request().source().aggregations();
                BytesReference br = new BytesArray(sourceBuilder.toString());
                Map<String, Object> jsonXContent = XContentHelper.convertToMap(br, false, MediaTypeRegistry.JSON).v2();
                if (jsonXContent.containsKey("aggregations")) {
                    // TODO: Add support for nested aggregations
                    throw new UnsupportedOperationException(" Nested Aggregations not supported");
                }

                Map.Entry<String, Object> metric = ((Map<String, Object>) jsonXContent.get(name)).entrySet().iterator().next();
                if (supportedMetrics.contains(MetricStat.fromTypeName(metric.getKey())) == false) {
                    throw new UnsupportedOperationException("Aggregation type not supported from star-tree: " + metric.getKey());
                }

                String queryField = ((Map<String, String>) metric.getValue()).get("field").toString();
                assert (supportedFields.contains(queryField));
                // TODO: create a star tree query and then invoke aggregator



//                CompositeIndexReader indexReader = new Composite90DocValuesReader()
                StarTreeAggregator starTreeAggregator = new StarTreeAggregator(name, factories, null, null, searchContext, parent, metadata, List.of(queryField), List.of(metric.getKey()));
                return starTreeAggregator;


            } catch (Exception e) {
                // Ignore
                System.out.println("Cannot use star tree index");

            }
        }
        return doCreateInternal(searchContext, parent, cardinality, metadata);
    }

    /**
     * Create the {@linkplain Aggregator} for a {@link ValuesSource} that
     * doesn't have values.
     */
    protected abstract Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata)
        throws IOException;

    /**
     * Create the {@linkplain Aggregator} for a {@link ValuesSource} that has
     * values.
     *
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    protected abstract Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException;

    @Override
    public String getStatsSubtype() {
        return config.valueSourceType().typeName();
    }

    public String getField() {
        return config.fieldContext().field();
    }

    public String getAggregationName() {
        return name;
    }

    public ValuesSourceConfig getConfig() {
        return config;
    }
}

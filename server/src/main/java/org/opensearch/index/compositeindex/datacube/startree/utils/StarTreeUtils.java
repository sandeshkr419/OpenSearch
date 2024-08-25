/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.search.aggregations.metrics.AvgAggregatorFactory;
import org.opensearch.search.aggregations.metrics.MaxAggregatorFactory;
import org.opensearch.search.aggregations.metrics.MinAggregatorFactory;
import org.opensearch.search.aggregations.metrics.SumAggregatorFactory;
import org.opensearch.search.aggregations.metrics.ValueCountAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Util class for building star tree
 *
 * @opensearch.experimental
 */
public class StarTreeUtils {

    private StarTreeUtils() {}

    public static final int ALL = -1;

    /**
     * The suffix appended to dimension field names in the Star Tree index.
     */
    public static final String DIMENSION_SUFFIX = "dim";

    /**
     * The suffix appended to metric field names in the Star Tree index.
     */
    public static final String METRIC_SUFFIX = "metric";

    /**
     * Map to associate star-tree supported AggregatorFactory classes with their corresponding MetricStat
     */
    public static final Map<Class<? extends ValuesSourceAggregatorFactory>, MetricStat> aggregatorStatMap = Map.of(
        SumAggregatorFactory.class,
        MetricStat.SUM,
        MaxAggregatorFactory.class,
        MetricStat.MAX,
        MinAggregatorFactory.class,
        MetricStat.MIN,
        ValueCountAggregatorFactory.class,
        MetricStat.VALUE_COUNT,
        AvgAggregatorFactory.class,
        MetricStat.AVG
    );

    /**
     * Returns the full field name for a dimension in the star-tree index.
     *
     * @param starTreeFieldName star-tree field name
     * @param dimensionName     name of the dimension
     * @return full field name for the dimension in the star-tree index
     */
    public static String fullyQualifiedFieldNameForStarTreeDimensionsDocValues(String starTreeFieldName, String dimensionName) {
        return starTreeFieldName + "_" + dimensionName + "_" + DIMENSION_SUFFIX;
    }

    /**
     * Returns the full field name for a metric in the star-tree index.
     *
     * @param starTreeFieldName star-tree field name
     * @param fieldName         name of the metric field
     * @param metricName        name of the metric
     * @return full field name for the metric in the star-tree index
     */
    public static String fullyQualifiedFieldNameForStarTreeMetricsDocValues(String starTreeFieldName, String fieldName, String metricName) {
        return MetricAggregatorInfo.toFieldName(starTreeFieldName, fieldName, metricName) + "_" + METRIC_SUFFIX;
    }

    /**
     * Get field infos from field names
     *
     * @param fields field names
     * @return field infos
     */
    public static FieldInfo[] getFieldInfoList(List<String> fields) {
        FieldInfo[] fieldInfoList = new FieldInfo[fields.size()];

        // field number is not really used. We depend on unique field names to get the desired iterator
        int fieldNumber = 0;

        for (String fieldName : fields) {
            fieldInfoList[fieldNumber] = getFieldInfo(fieldName, fieldNumber);
            fieldNumber++;
        }
        return fieldInfoList;
    }

    /**
     * Get new field info instance for a given field name and field number
     * @param fieldName name of the field
     * @param fieldNumber number of the field
     * @return new field info instance
     */
    public static FieldInfo getFieldInfo(String fieldName, int fieldNumber) {
        return new FieldInfo(
            fieldName,
            fieldNumber,
            false,
            false,
            true,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
            DocValuesType.SORTED_NUMERIC,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

}

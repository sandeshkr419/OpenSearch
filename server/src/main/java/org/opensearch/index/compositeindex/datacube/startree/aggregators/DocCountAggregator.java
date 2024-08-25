/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * Aggregator to handle '_doc_count' field
 *
 * @opensearch.experimental
 */
public class DocCountAggregator implements ValueAggregator<Long> {

    private static final StarTreeNumericType VALUE_AGGREGATOR_TYPE = StarTreeNumericType.LONG;

    public DocCountAggregator() {}

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    /**
     * If _doc_count field for a doc is missing, we increment the _doc_count by '1' for the associated doc
     * otherwise take the actual value present in the field
     */
    @Override
    public Long getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue) {
        if (segmentDocValue == null) {
            return getIdentityMetricValue();
        }
        return segmentDocValue;
    }

    @Override
    public Long mergeAggregatedValueAndSegmentValue(Long value, Long segmentDocValue) {
        assert value != null;
        return mergeAggregatedValues(value, segmentDocValue);
    }

    @Override
    public Long mergeAggregatedValues(Long value, Long aggregatedValue) {
        if (value == null) {
            value = getIdentityMetricValue();
        }
        if (aggregatedValue == null) {
            aggregatedValue = getIdentityMetricValue();
        }
        return value + aggregatedValue;
    }

    @Override
    public Long getInitialAggregatedValue(Long value) {
        return ValueAggregator.super.getInitialAggregatedValue(value);
    }

    @Override
    public Long toAggregatedValueType(Long rawValue) {
        return rawValue;
    }

    /**
     * If _doc_count field for a doc is missing, we increment the _doc_count by '1' for the associated doc
     */
    @Override
    public Long getIdentityMetricValue() {
        return 1L;
    }
}

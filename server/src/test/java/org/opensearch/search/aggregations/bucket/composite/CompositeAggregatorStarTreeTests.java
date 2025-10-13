/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.composite;

import org.opensearch.search.aggregations.StarTreePreComputeCollector;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for star-tree pre-computation support in CompositeAggregator
 */
public class CompositeAggregatorStarTreeTests extends OpenSearchTestCase {

    public void testCompositeAggregatorImplementsStarTreePreComputeCollector() {
        // Verify that CompositeAggregator implements the StarTreePreComputeCollector interface
        assertTrue(
            "CompositeAggregator should implement StarTreePreComputeCollector",
            StarTreePreComputeCollector.class.isAssignableFrom(CompositeAggregator.class)
        );
    }

    public void testStarTreePreComputeCollectorMethods() {
        // Verify that the required methods exist
        try {
            CompositeAggregator.class.getMethod(
                "getStarTreeBucketCollector",
                org.apache.lucene.index.LeafReaderContext.class,
                org.opensearch.index.codec.composite.CompositeIndexFieldInfo.class,
                org.opensearch.search.aggregations.StarTreeBucketCollector.class
            );

            CompositeAggregator.class.getMethod("getDimensionFilters");

        } catch (NoSuchMethodException e) {
            fail("CompositeAggregator should have the required StarTreePreComputeCollector methods: " + e.getMessage());
        }
    }
}

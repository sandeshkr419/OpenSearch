/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.meta;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricEntry;

import java.io.IOException;
import java.util.List;

public interface TreeMetadata {

    int readDimensionsCount() throws IOException;

    List<Integer> readStarTreeDimensions() throws IOException;

    int readMetricsCount() throws IOException;

    List<MetricEntry> readMetricPairs() throws IOException;

    int readSegmentAggregatedDocCount() throws IOException;

    long readDataStartFilePointer() throws IOException;

    long readDataLength() throws IOException;

}

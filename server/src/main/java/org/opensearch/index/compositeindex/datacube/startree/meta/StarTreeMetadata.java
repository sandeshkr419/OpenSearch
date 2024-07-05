/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.meta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricEntry;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Off heap implementation of {@link StarTreeNode}
 */
public class StarTreeMetadata implements TreeMetadata {
    private static final Logger logger = LogManager.getLogger(TreeMetadata.class);
    private final IndexInput meta;
    private final String starTreeFieldName;
    private final String starTreeFieldType;
    private final List<Integer> dimensionOrdinals;
    private final List<MetricEntry> metricEntries;
    private final Integer segmentAggregatedDocCount;
    private final long dataStartFilePointer;
    private final long dataLength;

    public StarTreeMetadata(IndexInput meta, String compositeFieldName, String compositeFieldType) throws IOException {
        this.meta = meta;
        try {
            this.starTreeFieldName = compositeFieldName;
            this.starTreeFieldType = compositeFieldType;
            this.dimensionOrdinals = readStarTreeDimensions();
            this.metricEntries = readMetricPairs();
            this.segmentAggregatedDocCount = readSegmentAggregatedDocCount();
            this.dataStartFilePointer = readDataStartFilePointer();
            this.dataLength = readDataLength();
        } catch (Exception e) {
            logger.error("Unable to read star-tree metadata from the file");
            throw new CorruptIndexException("Unable to read star-tree metadata from the file", meta);
        }
    }

    @Override
    public int readDimensionsCount() throws IOException {
        return meta.readInt();
    }

    @Override
    public List<Integer> readStarTreeDimensions() throws IOException {
        int dimensionCount = readDimensionsCount();
        List<Integer> dimensionOrdinals = new ArrayList<>();

        for (int i = 0; i < dimensionCount; i++) {
            dimensionOrdinals.add(meta.readInt());
        }

        return dimensionOrdinals;
    }

    @Override
    public int readMetricsCount() throws IOException {
        return meta.readInt();
    }

    @Override
    public List<MetricEntry> readMetricPairs() throws IOException {
        int metricCount = readMetricsCount();
        List<MetricEntry> metricEntries = new ArrayList<>();

        for (int i = 0; i < metricCount; i++) {
            String metricName = meta.readString();
            String metricStat = meta.readString();
            metricEntries.add(new MetricEntry(metricName, MetricStat.fromTypeName(metricStat)));
        }

        return metricEntries;
    }

    @Override
    public int readSegmentAggregatedDocCount() throws IOException {
        return meta.readInt();
    }

    @Override
    public long readDataStartFilePointer() throws IOException {
        return meta.readLong();
    }

    @Override
    public long readDataLength() throws IOException {
        return meta.readLong();
    }

    public String getStarTreeFieldName() {
        return starTreeFieldName;
    }

    public String getStarTreeFieldType() {
        return starTreeFieldType;
    }

    public List<Integer> getDimensionOrdinals() {
        return dimensionOrdinals;
    }

    public List<MetricEntry> getMetricPairs() {
        return metricEntries;
    }

    public Integer getSegmentAggregatedDocCount() {
        return segmentAggregatedDocCount;
    }

    public long getDataStartFilePointer() {
        return dataStartFilePointer;
    }

    public long getDataLength() {
        return dataLength;
    }
}

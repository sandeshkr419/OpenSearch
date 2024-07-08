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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds the associated metadata for the building of star-tree
 *
 * @opensearch.experimental
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
            this.metricEntries = readMetricEntries();
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
    public List<MetricEntry> readMetricEntries() throws IOException {
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

    /**
     * Returns the name of the star-tree field.
     *
     * @return star-tree field name
     */
    public String getStarTreeFieldName() {
        return starTreeFieldName;
    }

    /**
     * Returns the type of the star tree field.
     *
     * @return star-tree field type
     */
    public String getStarTreeFieldType() {
        return starTreeFieldType;
    }

    /**
     * Returns the list of dimension ordinals.
     *
     * @return star-tree dimension ordinals
     */
    public List<Integer> getDimensionOrdinals() {
        return dimensionOrdinals;
    }

    /**
     * Returns the list of metric entries.
     *
     * @return star-tree metric entries
     */
    public List<MetricEntry> getMetricEntries() {
        return metricEntries;
    }

    /**
     * Returns the aggregated document count for the star-tree.
     *
     * @return the aggregated document count for the star-tree.
     */
    public Integer getSegmentAggregatedDocCount() {
        return segmentAggregatedDocCount;
    }

    /**
     * Returns the file pointer to the start of the star-tree data.
     *
     * @return start file pointer for star-tree data
     */
    public long getDataStartFilePointer() {
        return dataStartFilePointer;
    }

    /**
     * Returns the length of star-tree data
     *
     * @return star-tree length
     */
    public long getDataLength() {
        return dataLength;
    }
}

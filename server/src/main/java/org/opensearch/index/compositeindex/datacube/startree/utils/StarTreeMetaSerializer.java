/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.MAGIC_MARKER;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.VERSION;

public class StarTreeMetaSerializer {

    private static final Logger logger = LogManager.getLogger(StarTreeMetaSerializer.class);

    public static void serializeStarTreeMetadata(
        IndexOutput metaOut,
        CompositeMappedFieldType.CompositeFieldType compositeFieldType,
        StarTreeField starTreeField,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        Integer segmentAggregatedCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {
        long totalSizeInBytes = 0;

        totalSizeInBytes += computeHeaderByteSize(compositeFieldType, starTreeField.getName()); // header size
        totalSizeInBytes += Integer.BYTES;                                                      // number of dimensions
        totalSizeInBytes += (long) starTreeField.getDimensionsOrder().size() * Integer.BYTES;   // dimension ids
        totalSizeInBytes += Integer.BYTES;                                                      // metric count
        totalSizeInBytes += computeMetricPairSizeInBytes(metricAggregatorInfos);                // metric - metric stat pair
        totalSizeInBytes += Integer.BYTES;                                                      // segment aggregated document count
        totalSizeInBytes += Long.BYTES;                                                         // data start file pointer
        totalSizeInBytes += Long.BYTES;                                                         // data length

        logger.info("Star tree size in bytes : {}", totalSizeInBytes);

        writeMetaHeader(metaOut, compositeFieldType, starTreeField.getName());
        writeMeta(metaOut, writeState, metricAggregatorInfos, starTreeField, segmentAggregatedCount, dataFilePointer, dataFileLength);
    }

    private static long computeMetricPairSizeInBytes(List<MetricAggregatorInfo> metricAggregatorInfos) {

        long metricPairSize = 0;

        for (MetricAggregatorInfo metricAggregatorInfo : metricAggregatorInfos) {
            metricPairSize += metricAggregatorInfo.getMetric().getBytes(UTF_8).length;
            metricPairSize += metricAggregatorInfo.getMetricStat().getTypeName().getBytes(UTF_8).length;
        }

        return metricPairSize;
    }

    private static int computeHeaderByteSize(CompositeMappedFieldType.CompositeFieldType compositeFieldType, String starTreeFieldName) {
        // Magic marker (8), version (4), size of header (4)
        int headerSizeInBytes = 16;

        // For star-tree field name
        headerSizeInBytes += starTreeFieldName.getBytes(UTF_8).length;

        // For star tree field type
        headerSizeInBytes += compositeFieldType.getName().getBytes(UTF_8).length;

        return headerSizeInBytes;
    }

    private static void writeMetaHeader(
        IndexOutput metaOut,
        CompositeMappedFieldType.CompositeFieldType compositeFieldType,
        String starTreeFieldName
    ) throws IOException {
        // magic marker for sanity
        metaOut.writeLong(MAGIC_MARKER);

        // version
        metaOut.writeInt(VERSION);

        // star tree field name
        metaOut.writeString(starTreeFieldName);

        // star tree field type
        metaOut.writeString(compositeFieldType.getName());
    }

    private static void writeMeta(
        IndexOutput metaOut,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        StarTreeField starTreeField,
        Integer segmentAggregatedDocCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {

        // number of dimensions
        metaOut.writeInt(starTreeField.getDimensionsOrder().size());

        // dimensions
        for (Dimension dimension : starTreeField.getDimensionsOrder()) {
            int dimensionFieldNumber = writeState.fieldInfos.fieldInfo(dimension.getField()).getFieldNumber();
            metaOut.writeInt(dimensionFieldNumber);
        }

        // number of metrics
        metaOut.writeInt(metricAggregatorInfos.size());

        // metric - metric stat pair
        for (MetricAggregatorInfo metricAggregatorInfo : metricAggregatorInfos) {
            String metricName = metricAggregatorInfo.getMetric();
            String metricStatName = metricAggregatorInfo.getMetricStat().getTypeName();
            metaOut.writeString(metricName);
            metaOut.writeString(metricStatName);
        }

        // segment aggregated document count
        metaOut.writeInt(segmentAggregatedDocCount);

        // star-tree data file pointer
        metaOut.writeLong(dataFilePointer);

        // star-tree data file length
        metaOut.writeLong(dataFileLength);

    }
}

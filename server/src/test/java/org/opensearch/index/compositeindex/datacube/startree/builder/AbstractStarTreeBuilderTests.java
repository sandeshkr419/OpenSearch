/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.codec.composite.Composite99DocValuesFormat;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.codec.composite.datacube.startree.fileformats.data.StarTreeDataWriter;
import org.opensearch.index.codec.composite.datacube.startree.fileformats.meta.MetricEntry;
import org.opensearch.index.codec.composite.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.CompositeIndexConstants;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTree;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.VERSION;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.ALL;
import static org.opensearch.index.mapper.CompositeMappedFieldType.CompositeFieldType.STAR_TREE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractStarTreeBuilderTests extends OpenSearchTestCase {
    protected MapperService mapperService;
    protected List<Dimension> dimensionsOrder;
    protected List<String> fields = List.of();
    protected List<Metric> metrics;
    protected Directory directory;
    protected FieldInfo[] fieldsInfo;
    protected StarTreeField compositeField;
    protected Map<String, DocValuesProducer> fieldProducerMap;
    protected SegmentWriteState writeState;
    protected BaseStarTreeBuilder builder;
    protected IndexOutput dataOut;
    protected IndexOutput metaOut;
    protected DocValuesConsumer docValuesConsumer;
    protected String dataFileName;
    protected String metaFileName;

    @Before
    public void setup() throws IOException {
        fields = List.of("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10");

        dimensionsOrder = List.of(
            new NumericDimension("field1"),
            new NumericDimension("field3"),
            new NumericDimension("field5"),
            new NumericDimension("field8")
        );
        metrics = List.of(
            new Metric("field2", List.of(MetricStat.SUM)),
            new Metric("field4", List.of(MetricStat.SUM)),
            new Metric("field6", List.of(MetricStat.COUNT)),
            new Metric("field9", List.of(MetricStat.MIN)),
            new Metric("field10", List.of(MetricStat.MAX))
        );

        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);

        compositeField = new StarTreeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldConfiguration(1, Set.of("field8"), getBuildMode())
        );
        directory = newFSDirectory(createTempDir());

        fieldsInfo = new FieldInfo[fields.size()];
        fieldProducerMap = new HashMap<>();
        for (int i = 0; i < fieldsInfo.length; i++) {
            fieldsInfo[i] = new FieldInfo(
                fields.get(i),
                i,
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
            fieldProducerMap.put(fields.get(i), docValuesProducer);
        }
        writeState = getWriteState(5);

        dataFileName = IndexFileNames.segmentFileName(
            writeState.segmentInfo.name,
            writeState.segmentSuffix,
            Composite99DocValuesFormat.DATA_EXTENSION
        );
        dataOut = writeState.directory.createOutput(dataFileName, writeState.context);

        metaFileName = IndexFileNames.segmentFileName(
            writeState.segmentInfo.name,
            writeState.segmentSuffix,
            Composite99DocValuesFormat.META_EXTENSION
        );
        metaOut = writeState.directory.createOutput(metaFileName, writeState.context);

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder("field6", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper4 = new NumberFieldMapper.Builder("field9", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper5 = new NumberFieldMapper.Builder("field10", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3, numberFieldMapper4, numberFieldMapper5),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        docValuesConsumer = mock(DocValuesConsumer.class);
    }

    private SegmentReadState getReadState(int numDocs) {
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            numDocs,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );
        return new SegmentReadState(segmentInfo.dir, segmentInfo, fieldInfos, null);
    }

    private SegmentWriteState getWriteState(int numDocs) {
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            numDocs,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );
        return new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));
    }

    public abstract BaseStarTreeBuilder getStarTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException;

    public void test_sortAndAggregateStarTreeDocuments() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }
        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);
    }

    SequentialDocValuesIterator[] getDimensionIterators(StarTreeDocument[] starTreeDocuments) {
        SequentialDocValuesIterator[] sequentialDocValuesIterators =
            new SequentialDocValuesIterator[starTreeDocuments[0].dimensions.length];
        for (int j = 0; j < starTreeDocuments[0].dimensions.length; j++) {
            List<Long> dimList = new ArrayList<>();
            List<Integer> docsWithField = new ArrayList<>();

            for (int i = 0; i < starTreeDocuments.length; i++) {
                if (starTreeDocuments[i].dimensions[j] != null) {
                    dimList.add(starTreeDocuments[i].dimensions[j]);
                    docsWithField.add(i);
                }
            }
            sequentialDocValuesIterators[j] = new SequentialDocValuesIterator(getSortedNumericMock(dimList, docsWithField));
        }
        return sequentialDocValuesIterators;
    }

    List<SequentialDocValuesIterator> getMetricIterators(StarTreeDocument[] starTreeDocuments) {
        List<SequentialDocValuesIterator> sequentialDocValuesIterators = new ArrayList<>();
        for (int j = 0; j < starTreeDocuments[0].metrics.length; j++) {
            List<Long> metricslist = new ArrayList<>();
            List<Integer> docsWithField = new ArrayList<>();

            for (int i = 0; i < starTreeDocuments.length; i++) {
                if (starTreeDocuments[i].metrics[j] != null) {
                    metricslist.add((long) starTreeDocuments[i].metrics[j]);
                    docsWithField.add(i);
                }
            }
            sequentialDocValuesIterators.add(new SequentialDocValuesIterator(getSortedNumericMock(metricslist, docsWithField)));
        }
        return sequentialDocValuesIterators;
    }

    public void test_sortAndAggregateStarTreeDocuments_nullMetric() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, null, randomDouble(), 8.0, 13.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 18.0, 3L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
        }
    }

    public void test_sortAndAggregateStarTreeDocuments_nullMetricField() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, null, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, null, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, null, randomDouble(), 6.0, 24.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, null, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, null, randomDouble(), 8.0, 13.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 0.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 0.0, 3L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/14813")
    public void test_sortAndAggregateStarTreeDocuments_nullAndMinusOneInDimensionField() throws IOException {
        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, null, 3L, 4L },
            new Double[] { 12.0, null, randomDouble(), 8.0, 20.0 }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { null, 4L, 2L, 1L },
            new Double[] { 10.0, null, randomDouble(), 12.0, 10.0 }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, 4L, 2L, 1L },
            new Double[] { 14.0, null, randomDouble(), 6.0, 24.0 }
        );
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, null, 3L, 4L }, new Double[] { 9.0, null, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { -1L, 4L, 2L, 1L }, new Double[] { 11.0, null, randomDouble(), 8.0, 13.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, null, 3L, 4L }, new Object[] { 21.0, 0.0, 2L }),
            new StarTreeDocument(new Long[] { null, 4L, 2L, 1L }, new Object[] { 24.0, 0.0, 2L }),
            new StarTreeDocument(new Long[] { -1L, 4L, 2L, 1L }, new Object[] { 11.0, 0.0, 1L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        for (StarTreeDocument resultStarTreeDocument : builder.getStarTreeDocuments()) {
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
        }
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 4, 1, builder.getStarTreeDocuments());
    }

    public void test_sortAndAggregateStarTreeDocuments_nullDimensionsAndNullMetrics() throws IOException {
        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(new Long[] { null, null, null, null }, new Double[] { null, null, null, null, null });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { null, null, null, null }, new Double[] { null, null, null, null, null });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { null, null, null, null }, new Double[] { null, null, null, null, null });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { null, null, null, null }, new Double[] { null, null, null, null, null });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { null, null, null, null }, new Double[] { null, null, null, null, null });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { 0.0, 0.0, 0L, null, null })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long metric1 = starTreeDocuments[i].metrics[0] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0])
                : null;
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            Long metric3 = starTreeDocuments[i].metrics[2] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2])
                : null;
            Long metric4 = starTreeDocuments[i].metrics[3] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3])
                : null;
            Long metric5 = starTreeDocuments[i].metrics[4] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4])
                : null;
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
        }
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 4, 1, builder.getStarTreeDocuments());
    }

    public void test_sortAndAggregateStarTreeDocuments_nullDimensionsAndFewNullMetrics() throws IOException {
        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        double sumValue = randomDouble();
        double minValue = randomDouble();
        double maxValue = randomDouble();

        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Double[] { null, null, randomDouble(), null, maxValue }
        );
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { null, null, null, null }, new Double[] { null, null, null, null, null });
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Double[] { null, null, null, minValue, null }
        );
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { null, null, null, null }, new Double[] { null, null, null, null, null });
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Double[] { sumValue, null, randomDouble(), null, null }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { sumValue, 0.0, 2L, minValue, maxValue })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long metric1 = starTreeDocuments[i].metrics[0] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0])
                : null;
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            Long metric3 = starTreeDocuments[i].metrics[2] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2])
                : null;
            Long metric4 = starTreeDocuments[i].metrics[3] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3])
                : null;
            Long metric5 = starTreeDocuments[i].metrics[4] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4])
                : null;
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
        }
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 4, 1, builder.getStarTreeDocuments());
    }

    public void test_sortAndAggregateStarTreeDocuments_emptyDimensions() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        // Setting second metric iterator as empty sorted numeric , indicating a metric field is null
        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Double[] { 12.0, null, randomDouble(), 8.0, 20.0 }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Double[] { 10.0, null, randomDouble(), 12.0, 10.0 }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Double[] { 14.0, null, randomDouble(), 6.0, 24.0 }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Double[] { 9.0, null, randomDouble(), 9.0, 12.0 }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { null, null, null, null },
            new Double[] { 11.0, null, randomDouble(), 8.0, 13.0 }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { 56.0, 0.0, 5L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            Long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            Long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            Long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Object[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }
        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );

        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
        }
    }

    public void test_sortAndAggregateStarTreeDocument_longMaxAndLongMinDimensions() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { Long.MIN_VALUE, 4L, 3L, 4L },
            new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Double[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { Long.MIN_VALUE, 4L, 3L, 4L },
            new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { Long.MIN_VALUE, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, Long.MAX_VALUE }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);

    }

    public void test_sortAndAggregateStarTreeDocument_DoubleMaxAndDoubleMinMetrics() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Double[] { Double.MAX_VALUE, 10.0, randomDouble(), 8.0, 20.0 }
        );
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Double[] { 14.0, Double.MIN_VALUE, randomDouble(), 6.0, 24.0 }
        );
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { Double.MAX_VALUE + 9, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, Double.MIN_VALUE + 22, 3L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 3, 1, builder.getStarTreeDocuments());

    }

    public void test_build_halfFloatMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder("field6", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper4 = new NumberFieldMapper.Builder("field9", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper5 = new NumberFieldMapper.Builder(
            "field10",
            NumberFieldMapper.NumberType.HALF_FLOAT,
            false,
            true
        ).build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3, numberFieldMapper4, numberFieldMapper5),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf1", 12),
                new HalfFloatPoint("hf6", 10),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 8),
                new HalfFloatPoint("field10", 20) }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf2", 10),
                new HalfFloatPoint("hf7", 6),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 12),
                new HalfFloatPoint("field10", 10) }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf3", 14),
                new HalfFloatPoint("hf8", 12),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 6),
                new HalfFloatPoint("field10", 24) }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf4", 9),
                new HalfFloatPoint("hf9", 4),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 9),
                new HalfFloatPoint("field10", 12) }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf5", 11),
                new HalfFloatPoint("hf10", 16),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 8),
                new HalfFloatPoint("field10", 13) }
        );

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[0]).numericValue().floatValue()
            );
            long metric2 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[1]).numericValue().floatValue()
            );
            long metric3 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[2]).numericValue().floatValue()
            );
            long metric4 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[3]).numericValue().floatValue()
            );
            long metric5 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[4]).numericValue().floatValue()
            );
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);

        metaOut.close();
        dataOut.close();

        // length of the data is 1303 bytes. 16 bytes for header. Each Node is 33 bytes. We have 39 nodes ~ 33 * 39 = 1287. Total = 1303.
        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "test",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(
                new MetricEntry("field2", MetricStat.SUM),
                new MetricEntry("field4", MetricStat.SUM),
                new MetricEntry("field6", MetricStat.COUNT),
                new MetricEntry("field9", MetricStat.MIN),
                new MetricEntry("field10", MetricStat.MAX)
            ),
            2,
            1,
            Set.of("field8"),
            getBuildMode(),
            0,
            346
        );

        validateStarTreeFileFormats(builder.getRootNode(), noOfStarTreeDocuments, starTreeMetadata);
    }

    public void test_build_floatMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder("field6", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper4 = new NumberFieldMapper.Builder("field9", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper5 = new NumberFieldMapper.Builder("field10", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3, numberFieldMapper4, numberFieldMapper5),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Float[] { 12.0F, 10.0F, randomFloat(), 8.0F, 20.0F }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Float[] { 10.0F, 6.0F, randomFloat(), 12.0F, 10.0F }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Float[] { 14.0F, 12.0F, randomFloat(), 6.0F, 24.0F }
        );
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Float[] { 9.0F, 4.0F, randomFloat(), 9.0F, 12.0F });
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Float[] { 11.0F, 16.0F, randomFloat(), 8.0F, 13.0F }
        );

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);

        metaOut.close();
        dataOut.close();

        // length of the data is 1303 bytes. 16 bytes for header. Each Node is 33 bytes. We have 39 nodes ~ 33 * 39 = 1287. Total = 1303.
        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "test",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(
                new MetricEntry("field2", MetricStat.SUM),
                new MetricEntry("field4", MetricStat.SUM),
                new MetricEntry("field6", MetricStat.COUNT),
                new MetricEntry("field9", MetricStat.MIN),
                new MetricEntry("field10", MetricStat.MAX)
            ),
            2,
            1,
            Set.of("field8"),
            getBuildMode(),
            0,
            346
        );

        validateStarTreeFileFormats(builder.getRootNode(), noOfStarTreeDocuments, starTreeMetadata);
    }

    abstract StarTreeFieldConfiguration.StarTreeBuildMode getBuildMode();

    public void test_build_longMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder("field6", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper4 = new NumberFieldMapper.Builder("field9", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper5 = new NumberFieldMapper.Builder("field10", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3, numberFieldMapper4, numberFieldMapper5),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Long[] { 12L, 10L, randomLong(), 8L, 20L });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 10L, 6L, randomLong(), 12L, 10L });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 14L, 12L, randomLong(), 6L, 24L });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Long[] { 9L, 4L, randomLong(), 9L, 12L });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 11L, 16L, randomLong(), 8L, 13L });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = (Long) starTreeDocuments[i].metrics[0];
            long metric2 = (Long) starTreeDocuments[i].metrics[1];
            long metric3 = (Long) starTreeDocuments[i].metrics[2];
            long metric4 = (Long) starTreeDocuments[i].metrics[3];
            long metric5 = (Long) starTreeDocuments[i].metrics[4];
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);

        metaOut.close();
        dataOut.close();

        // length of the data is 1303 bytes. 16 bytes for header. Each Node is 33 bytes. We have 39 nodes ~ 33 * 39 = 1287. Total = 1303.
        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "test",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(
                new MetricEntry("field2", MetricStat.SUM),
                new MetricEntry("field4", MetricStat.SUM),
                new MetricEntry("field6", MetricStat.COUNT),
                new MetricEntry("field9", MetricStat.MIN),
                new MetricEntry("field10", MetricStat.MAX)
            ),
            2,
            1,
            Set.of("field8"),
            getBuildMode(),
            0,
            346
        );

        validateStarTreeFileFormats(builder.getRootNode(), noOfStarTreeDocuments, starTreeMetadata);
    }

    private static Iterator<StarTreeDocument> getExpectedStarTreeDocumentIterator() {
        List<StarTreeDocument> expectedStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 }),
            new StarTreeDocument(new Long[] { null, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 }),
            new StarTreeDocument(new Long[] { null, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { null, 4L, null, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 }),
            new StarTreeDocument(new Long[] { null, 4L, null, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { null, 4L, null, null }, new Object[] { 56.0, 48.0, 5L, 6.0, 24.0 })
        );
        return expectedStarTreeDocuments.iterator();
    }

    public void test_build_multipleStarTrees() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
        builder.close();

        // building another tree in the same file
        fields = List.of("fieldC", "fieldB", "fieldL", "fieldI");

        dimensionsOrder = List.of(new NumericDimension("fieldC"), new NumericDimension("fieldB"), new NumericDimension("fieldL"));
        metrics = List.of(new Metric("fieldI", List.of(MetricStat.SUM)));

        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);

        compositeField = new StarTreeField("test", dimensionsOrder, metrics, new StarTreeFieldConfiguration(1, Set.of(), getBuildMode()));
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            7,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        fieldsInfo = new FieldInfo[fields.size()];
        fieldProducerMap = new HashMap<>();
        for (int i = 0; i < fieldsInfo.length; i++) {
            fieldsInfo[i] = new FieldInfo(
                fields.get(i),
                i,
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
            fieldProducerMap.put(fields.get(i), docValuesProducer);
        }
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        writeState = new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("fieldI", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);

        InMemoryTreeNode rootNode1 = builder.getRootNode();

        int noOfStarTreeDocuments2 = 7;
        StarTreeDocument[] starTreeDocuments2 = new StarTreeDocument[noOfStarTreeDocuments2];
        starTreeDocuments2[0] = new StarTreeDocument(new Long[] { 1L, 11L, 21L }, new Double[] { 400.0 });
        starTreeDocuments2[1] = new StarTreeDocument(new Long[] { 1L, 12L, 22L }, new Double[] { 200.0 });
        starTreeDocuments2[2] = new StarTreeDocument(new Long[] { 2L, 13L, 23L }, new Double[] { 300.0 });
        starTreeDocuments2[3] = new StarTreeDocument(new Long[] { 2L, 13L, 21L }, new Double[] { 100.0 });
        starTreeDocuments2[4] = new StarTreeDocument(new Long[] { 3L, 11L, 21L }, new Double[] { 600.0 });
        starTreeDocuments2[5] = new StarTreeDocument(new Long[] { 3L, 12L, 23L }, new Double[] { 200.0 });
        starTreeDocuments2[6] = new StarTreeDocument(new Long[] { 3L, 12L, 21L }, new Double[] { 400.0 });

        StarTreeDocument[] segmentStarTreeDocuments2 = new StarTreeDocument[noOfStarTreeDocuments2];
        for (int i = 0; i < noOfStarTreeDocuments2; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments2[i].metrics[0]);
            segmentStarTreeDocuments2[i] = new StarTreeDocument(starTreeDocuments2[i].dimensions, new Long[] { metric1 });
        }

        SequentialDocValuesIterator[] dimsIterators2 = getDimensionIterators(segmentStarTreeDocuments2);
        List<SequentialDocValuesIterator> metricsIterators2 = getMetricIterators(segmentStarTreeDocuments2);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator2 = builder.sortAndAggregateSegmentDocuments(
            dimsIterators2,
            metricsIterators2
        );
        builder.build(segmentStarTreeDocumentIterator2, new AtomicInteger(), mock(DocValuesConsumer.class));
        InMemoryTreeNode rootNode2 = builder.getRootNode();

        metaOut.close();
        dataOut.close();

        // length of the data is 1303 bytes. 16 bytes for header. Each Node is 33 bytes. We have 39 nodes ~ 33 * 39 = 1287. Total = 1303.
        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "test",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(
                new MetricEntry("field2", MetricStat.SUM),
                new MetricEntry("field4", MetricStat.SUM),
                new MetricEntry("field6", MetricStat.COUNT),
                new MetricEntry("field9", MetricStat.MIN),
                new MetricEntry("field10", MetricStat.MAX)
            ),
            2,
            1,
            Set.of("field8"),
            getBuildMode(),
            0,
            346
        );

        // length of the data is 1303 bytes. 16 bytes for header. Each Node is 33 bytes. We have 39 nodes ~ 33 * 39 = 1287. Total = 1303.
        StarTreeMetadata starTreeMetadata2 = new StarTreeMetadata(
            "test",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("fieldC", "fieldB", "fieldL"),
            List.of(new MetricEntry("fieldI", MetricStat.SUM)),
            7,
            1,
            Set.of(),
            getBuildMode(),
            346,
            1303
        );

        SegmentReadState readState = getReadState(3);

        IndexInput dataIn = readState.directory.openInput(dataFileName, IOContext.DEFAULT);
        IndexInput metaIn = readState.directory.openInput(metaFileName, IOContext.DEFAULT);

        validateFileFormats(dataIn, metaIn, rootNode1, starTreeMetadata);
        validateFileFormats(dataIn, metaIn, rootNode2, starTreeMetadata2);

        dataIn.close();
        metaIn.close();

    }

    public void test_build() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            long metric4 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[3]);
            long metric5 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[4]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(
                starTreeDocuments[i].dimensions,
                new Long[] { metric1, metric2, metric3, metric4, metric5 }
            );
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);

        metaOut.close();
        dataOut.close();

        // length of the data is 1303 bytes. 16 bytes for header. Each Node is 33 bytes. We have 39 nodes ~ 33 * 39 = 1287. Total = 1303.
        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "test",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(
                new MetricEntry("field2", MetricStat.SUM),
                new MetricEntry("field4", MetricStat.SUM),
                new MetricEntry("field6", MetricStat.COUNT),
                new MetricEntry("field9", MetricStat.MIN),
                new MetricEntry("field10", MetricStat.MAX)
            ),
            2,
            1,
            Set.of("field8"),
            getBuildMode(),
            0,
            346
        );

        validateStarTreeFileFormats(builder.getRootNode(), noOfStarTreeDocuments, starTreeMetadata);
    }

    private void assertStarTreeDocuments(
        List<StarTreeDocument> resultStarTreeDocuments,
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator
    ) {
        Iterator<StarTreeDocument> resultStarTreeDocumentIterator = resultStarTreeDocuments.iterator();
        while (resultStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = resultStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
        }
    }

    public void test_build_starTreeDataset() throws IOException {

        fields = List.of("fieldC", "fieldB", "fieldL", "fieldI");

        dimensionsOrder = List.of(new NumericDimension("fieldC"), new NumericDimension("fieldB"), new NumericDimension("fieldL"));
        metrics = List.of(new Metric("fieldI", List.of(MetricStat.SUM)));

        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);

        compositeField = new StarTreeField("test", dimensionsOrder, metrics, new StarTreeFieldConfiguration(1, Set.of(), getBuildMode()));
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            7,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        fieldsInfo = new FieldInfo[fields.size()];
        fieldProducerMap = new HashMap<>();
        for (int i = 0; i < fieldsInfo.length; i++) {
            fieldsInfo[i] = new FieldInfo(
                fields.get(i),
                i,
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
            fieldProducerMap.put(fields.get(i), docValuesProducer);
        }
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        writeState = new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("fieldI", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);

        int noOfStarTreeDocuments = 7;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 1L, 11L, 21L }, new Double[] { 400.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 1L, 12L, 22L }, new Double[] { 200.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 2L, 13L, 23L }, new Double[] { 300.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 13L, 21L }, new Double[] { 100.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 11L, 21L }, new Double[] { 600.0 });
        starTreeDocuments[5] = new StarTreeDocument(new Long[] { 3L, 12L, 23L }, new Double[] { 200.0 });
        starTreeDocuments[6] = new StarTreeDocument(new Long[] { 3L, 12L, 21L }, new Double[] { 400.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1 });
        }

        SequentialDocValuesIterator[] dimsIterators = getDimensionIterators(segmentStarTreeDocuments);
        List<SequentialDocValuesIterator> metricsIterators = getMetricIterators(segmentStarTreeDocuments);
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimsIterators,
            metricsIterators
        );
        builder.build(segmentStarTreeDocumentIterator, new AtomicInteger(), mock(DocValuesConsumer.class));

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = expectedStarTreeDocuments();
        Iterator<StarTreeDocument> resultStarTreeDocumentIterator = resultStarTreeDocuments.iterator();
        Map<Integer, Map<Long, Integer>> dimValueToDocIdMap = new HashMap<>();
        builder.rootNode.nodeType = StarTreeNodeType.STAR.getValue();
        traverseStarTree(builder.rootNode, dimValueToDocIdMap, true);

        Map<Integer, Map<Long, Double>> expectedDimToValueMap = getExpectedDimToValueMap();
        for (Map.Entry<Integer, Map<Long, Integer>> entry : dimValueToDocIdMap.entrySet()) {
            int dimId = entry.getKey();
            if (dimId == -1) continue;
            Map<Long, Double> map = expectedDimToValueMap.get(dimId);
            for (Map.Entry<Long, Integer> dimValueToDocIdEntry : entry.getValue().entrySet()) {
                long dimValue = dimValueToDocIdEntry.getKey();
                int docId = dimValueToDocIdEntry.getValue();
                if (map.get(dimValue) != null) {
                    assertEquals(map.get(dimValue), resultStarTreeDocuments.get(docId).metrics[0]);
                }
            }
        }

        while (resultStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = resultStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
        }

        metaOut.close();
        dataOut.close();

        validateStarTree(builder.getRootNode(), 3, 1, builder.getStarTreeDocuments());

        // length of the data is 1303 bytes. 16 bytes for header. Each Node is 33 bytes. We have 39 nodes ~ 33 * 39 = 1287. Total = 1303.
        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "test",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("fieldC", "fieldB", "fieldL"),
            List.of(new MetricEntry("fieldI", MetricStat.SUM)),
            7,
            1,
            Set.of(),
            getBuildMode(),
            0,
            1303
        );
        validateStarTreeFileFormats(builder.getRootNode(), 7, starTreeMetadata);
    }

    private void validateStarTreeFileFormats(InMemoryTreeNode rootNode, int numDocs, StarTreeMetadata expectedStarTreeMetadata)
        throws IOException {

        SegmentReadState readState = getReadState(numDocs);

        IndexInput dataIn = readState.directory.openInput(dataFileName, IOContext.DEFAULT);
        IndexInput metaIn = readState.directory.openInput(metaFileName, IOContext.DEFAULT);

        validateFileFormats(dataIn, metaIn, rootNode, expectedStarTreeMetadata);

        dataIn.close();
        metaIn.close();
    }

    private void validateFileFormats(
        IndexInput dataIn,
        IndexInput metaIn,
        InMemoryTreeNode rootNode,
        StarTreeMetadata expectedStarTreeMetadata
    ) throws IOException {
        long magicMarker = metaIn.readLong();
        assertEquals(COMPOSITE_FIELD_MARKER, magicMarker);
        int version = metaIn.readVInt();
        assertEquals(VERSION, version);

        String compositeFieldName = metaIn.readString();
        assertEquals(expectedStarTreeMetadata.getStarTreeFieldName(), compositeFieldName);
        CompositeMappedFieldType.CompositeFieldType compositeFieldType = CompositeMappedFieldType.CompositeFieldType.fromName(
            metaIn.readString()
        );
        assertEquals(STAR_TREE, compositeFieldType);
        StarTreeMetadata resultStarTreeMetadata = new StarTreeMetadata(metaIn, compositeFieldName, compositeFieldType);
        assertStarTreeMetadata(expectedStarTreeMetadata, resultStarTreeMetadata);

        IndexInput starTreeIndexInput = dataIn.slice(
            "star-tree data slice for respective star-tree fields",
            resultStarTreeMetadata.getDataStartFilePointer(),
            resultStarTreeMetadata.getDataLength()
        );
        StarTree starTree = new StarTree(starTreeIndexInput, resultStarTreeMetadata);

        StarTreeNode starTreeNode = starTree.getRoot();
        Queue<StarTreeNode> expectedTreeNodeQueue = new ArrayDeque<>();
        Queue<InMemoryTreeNode> resultTreeNodeQueue = new ArrayDeque<>();

        expectedTreeNodeQueue.add(starTreeNode);
        resultTreeNodeQueue.add(rootNode);

        while ((starTreeNode = expectedTreeNodeQueue.poll()) != null && (rootNode = resultTreeNodeQueue.poll()) != null) {

            // verify the star node
            assertStarTreeNode(starTreeNode, rootNode);

            Iterator<? extends StarTreeNode> expectedChildrenIterator = starTreeNode.getChildrenIterator();

            List<InMemoryTreeNode> sortedChildren = new ArrayList<>();
            if (rootNode.children != null) {
                sortedChildren = new ArrayList<>(rootNode.children.values());
            }
            sortedChildren.sort(
                Comparator.comparingInt(InMemoryTreeNode::getNodeType).thenComparingLong(InMemoryTreeNode::getDimensionValue)
            );

            if (starTreeNode.getChildDimensionId() != -1) {
                assertFalse(sortedChildren.isEmpty());
                int childCount = 0;
                while (expectedChildrenIterator.hasNext()) {
                    StarTreeNode child = expectedChildrenIterator.next();
                    InMemoryTreeNode resultChildNode = sortedChildren.get(childCount);

                    assertNotNull(resultChildNode);
                    assertNotNull(child);
                    assertStarTreeNode(child, resultChildNode);

                    expectedTreeNodeQueue.add(child);
                    resultTreeNodeQueue.add(resultChildNode);

                    childCount++;
                }
                assertEquals(childCount, rootNode.children.size());
            } else {
                assertNull(rootNode.children);
            }
        }

        assertTrue(expectedTreeNodeQueue.isEmpty());
        assertTrue(resultTreeNodeQueue.isEmpty());

    }

    private void assertStarTreeNode(StarTreeNode starTreeNode, InMemoryTreeNode treeNode) throws IOException {
        assertEquals(starTreeNode.getDimensionId(), treeNode.dimensionId);
        assertEquals(starTreeNode.getDimensionValue(), treeNode.dimensionValue);
        assertEquals(starTreeNode.getStartDocId(), treeNode.startDocId);
        assertEquals(starTreeNode.getEndDocId(), treeNode.endDocId);
        assertEquals(starTreeNode.getChildDimensionId(), treeNode.childDimensionId);
        assertEquals(starTreeNode.getAggregatedDocId(), treeNode.aggregatedDocId);

        if (starTreeNode.getChildDimensionId() != -1) {
            assertFalse(starTreeNode.isLeaf());
            if (treeNode.children != null) {
                assertEquals(starTreeNode.getNumChildren(), treeNode.children.values().size());
            }
        } else {
            assertTrue(starTreeNode.isLeaf());
        }

    }

    private void assertStarTreeMetadata(StarTreeMetadata expectedStarTreeMetadata, StarTreeMetadata resultStarTreeMetadata) {

        assertEquals(expectedStarTreeMetadata.getCompositeFieldName(), resultStarTreeMetadata.getCompositeFieldName());
        assertEquals(expectedStarTreeMetadata.getCompositeFieldType(), resultStarTreeMetadata.getCompositeFieldType());
        assertEquals(expectedStarTreeMetadata.getDimensionFields().size(), resultStarTreeMetadata.getDimensionFields().size());
        for (int i = 0; i < expectedStarTreeMetadata.getDimensionFields().size(); i++) {
            assertEquals(expectedStarTreeMetadata.getDimensionFields().get(i), resultStarTreeMetadata.getDimensionFields().get(i));
        }
        assertEquals(expectedStarTreeMetadata.getMetricEntries().size(), resultStarTreeMetadata.getMetricEntries().size());
        for (int i = 0; i < expectedStarTreeMetadata.getMetricEntries().size(); i++) {
            MetricEntry expectedMetricEntry = expectedStarTreeMetadata.getMetricEntries().get(i);
            MetricEntry resultMetricEntry = resultStarTreeMetadata.getMetricEntries().get(i);
            assertEquals(expectedMetricEntry, resultMetricEntry);
        }

        assertEquals(expectedStarTreeMetadata.getSegmentAggregatedDocCount(), resultStarTreeMetadata.getSegmentAggregatedDocCount());
        assertEquals(expectedStarTreeMetadata.getMaxLeafDocs(), resultStarTreeMetadata.getMaxLeafDocs());
        assertEquals(
            expectedStarTreeMetadata.getSkipStarNodeCreationInDims().size(),
            resultStarTreeMetadata.getSkipStarNodeCreationInDims().size()
        );
        for (String skipDimension : expectedStarTreeMetadata.getSkipStarNodeCreationInDims()) {
            assertTrue(resultStarTreeMetadata.getSkipStarNodeCreationInDims().contains(skipDimension));
        }
        assertEquals(expectedStarTreeMetadata.getStarTreeBuildMode(), resultStarTreeMetadata.getStarTreeBuildMode());
        assertEquals(expectedStarTreeMetadata.getDataStartFilePointer(), resultStarTreeMetadata.getDataStartFilePointer());
        assertEquals(expectedStarTreeMetadata.getDataLength(), resultStarTreeMetadata.getDataLength());
        assertEquals(0, (resultStarTreeMetadata.getDataLength() - StarTreeDataWriter.computeStarTreeDataHeaderByteSize()) % 33);
    }

    private static Map<Integer, Map<Long, Double>> getExpectedDimToValueMap() {
        Map<Integer, Map<Long, Double>> expectedDimToValueMap = new HashMap<>();
        Map<Long, Double> dimValueMap = new HashMap<>();
        dimValueMap.put(1L, 600.0);
        dimValueMap.put(2L, 400.0);
        dimValueMap.put(3L, 1200.0);
        expectedDimToValueMap.put(0, dimValueMap);

        dimValueMap = new HashMap<>();
        dimValueMap.put(11L, 1000.0);
        dimValueMap.put(12L, 800.0);
        dimValueMap.put(13L, 400.0);
        expectedDimToValueMap.put(1, dimValueMap);

        dimValueMap = new HashMap<>();
        dimValueMap.put(21L, 1500.0);
        dimValueMap.put(22L, 200.0);
        dimValueMap.put(23L, 500.0);
        expectedDimToValueMap.put(2, dimValueMap);
        return expectedDimToValueMap;
    }

    private Iterator<StarTreeDocument> expectedStarTreeDocuments() {
        List<StarTreeDocument> expectedStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 1L, 11L, 21L }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 1L, 12L, 22L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { 2L, 13L, 21L }, new Object[] { 100.0 }),
            new StarTreeDocument(new Long[] { 2L, 13L, 23L }, new Object[] { 300.0 }),
            new StarTreeDocument(new Long[] { 3L, 11L, 21L }, new Object[] { 600.0 }),
            new StarTreeDocument(new Long[] { 3L, 12L, 21L }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 3L, 12L, 23L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { null, 11L, 21L }, new Object[] { 1000.0 }),
            new StarTreeDocument(new Long[] { null, 12L, 21L }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { null, 12L, 22L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { null, 12L, 23L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { null, 13L, 21L }, new Object[] { 100.0 }),
            new StarTreeDocument(new Long[] { null, 13L, 23L }, new Object[] { 300.0 }),
            new StarTreeDocument(new Long[] { null, null, 21L }, new Object[] { 1500.0 }),
            new StarTreeDocument(new Long[] { null, null, 22L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { null, null, 23L }, new Object[] { 500.0 }),
            new StarTreeDocument(new Long[] { null, null, null }, new Object[] { 2200.0 }),
            new StarTreeDocument(new Long[] { null, 12L, null }, new Object[] { 800.0 }),
            new StarTreeDocument(new Long[] { null, 13L, null }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 1L, null, 21L }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 1L, null, 22L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { 1L, null, null }, new Object[] { 600.0 }),
            new StarTreeDocument(new Long[] { 2L, 13L, null }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 3L, null, 21L }, new Object[] { 1000.0 }),
            new StarTreeDocument(new Long[] { 3L, null, 23L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { 3L, null, null }, new Object[] { 1200.0 }),
            new StarTreeDocument(new Long[] { 3L, 12L, null }, new Object[] { 600.0 })
        );

        return expectedStarTreeDocuments.iterator();
    }

    public void testFlushFlow() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5);

        List<Long> metricsList = List.of(
            getLongFromDouble(0.0),
            getLongFromDouble(10.0),
            getLongFromDouble(20.0),
            getLongFromDouble(30.0),
            getLongFromDouble(40.0),
            getLongFromDouble(50.0)
        );
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5);

        StarTreeField sf = getStarTreeFieldWithMultipleMetrics();
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        SortedNumericDocValues m2sndv = getSortedNumericMock(metricsList, metricsWithField);

        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(6), mapperService);
        SequentialDocValuesIterator[] dimDvs = { new SequentialDocValuesIterator(d1sndv), new SequentialDocValuesIterator(d2sndv) };
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimDvs,
            List.of(new SequentialDocValuesIterator(m1sndv), new SequentialDocValuesIterator(m2sndv))
        );
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [metric], count [metric] ]
         [0, 0] | [0.0, 1]
         [1, 1] | [10.0, 1]
         [3, 3] | [30.0, 1]
         [4, 4] | [40.0, 1]
         [5, 5] | [50.0, 1]
         [null, 2] | [20.0, 1]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        int count = 0;
        for (StarTreeDocument starTreeDocument : starTreeDocuments) {
            count++;
            if (starTreeDocument.dimensions[1] != null) {
                assertEquals(
                    starTreeDocument.dimensions[0] == null
                        ? starTreeDocument.dimensions[1] * 1 * 10.0
                        : starTreeDocument.dimensions[0] * 10,
                    starTreeDocument.metrics[0]
                );
                assertEquals(1L, starTreeDocument.metrics[1]);
            } else {
                assertEquals(150D, starTreeDocument.metrics[0]);
                assertEquals(6L, starTreeDocument.metrics[1]);
            }
        }
        assertEquals(13, count);
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.SUM), new MetricEntry("field2", MetricStat.COUNT)),
            6,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            280
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);

    }

    public void testFlushFlowDimsReverse() throws IOException {
        List<Long> dimList = List.of(5L, 4L, 3L, 2L, 1L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4);
        List<Long> dimList2 = List.of(5L, 4L, 3L, 2L, 1L, 0L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5);

        List<Long> metricsList = List.of(
            getLongFromDouble(50.0),
            getLongFromDouble(40.0),
            getLongFromDouble(30.0),
            getLongFromDouble(20.0),
            getLongFromDouble(10.0),
            getLongFromDouble(0.0)
        );
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5);

        StarTreeField sf = getStarTreeFieldWithMultipleMetrics();
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        SortedNumericDocValues m2sndv = getSortedNumericMock(metricsList, metricsWithField);

        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(6), mapperService);
        SequentialDocValuesIterator[] dimDvs = { new SequentialDocValuesIterator(d1sndv), new SequentialDocValuesIterator(d2sndv) };
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimDvs,
            List.of(new SequentialDocValuesIterator(m1sndv), new SequentialDocValuesIterator(m2sndv))
        );
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [metric], count [metric] ]
         [1, 1] | [10.0, 1]
         [2, 2] | [20.0, 1]
         [3, 3] | [30.0, 1]
         [4, 4] | [40.0, 1]
         [5, 5] | [50.0, 1]
         [null, 0] | [0.0, 1]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            if (count <= 6) {
                count++;
                if (starTreeDocument.dimensions[0] != null) {
                    assertEquals(count, (long) starTreeDocument.dimensions[0]);
                }
                assertEquals(starTreeDocument.dimensions[1] * 10.0, starTreeDocument.metrics[0]);
                assertEquals(1L, starTreeDocument.metrics[1]);
            }
        }
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.SUM), new MetricEntry("field2", MetricStat.COUNT)),
            6,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            280
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testFlushFlowBuild() throws IOException {
        List<Long> dimList = new ArrayList<>(100);
        List<Integer> docsWithField = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            dimList.add((long) i);
            docsWithField.add(i);
        }

        List<Long> dimList2 = new ArrayList<>(100);
        List<Integer> docsWithField2 = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            dimList2.add((long) i);
            docsWithField2.add(i);
        }

        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1, new HashSet<>(), getBuildMode());
        StarTreeField sf = new StarTreeField("sf", dims, metrics, c);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);

        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(100), mapperService);

        DocValuesProducer d1vp = getDocValuesProducer(d1sndv);
        DocValuesProducer d2vp = getDocValuesProducer(d2sndv);
        DocValuesProducer m1vp = getDocValuesProducer(m1sndv);
        Map<String, DocValuesProducer> fieldProducerMap = Map.of("field1", d1vp, "field3", d2vp, "field2", m1vp);
        builder.build(fieldProducerMap, new AtomicInteger(), docValuesConsumer);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [ metric] ]
         [0, 0] | [0.0]
         [1, 1] | [10.0]
         [2, 2] | [20.0]
         [3, 3] | [30.0]
         [4, 4] | [40.0]
         ....
         [null, 0] | [0.0]
         [null, 1] | [10.0]
         ...
         [null, null] | [49500.0]
         */
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        for (StarTreeDocument starTreeDocument : starTreeDocuments) {
            assertEquals(
                starTreeDocument.dimensions[1] != null ? starTreeDocument.dimensions[1] * 10.0 : 49500.0,
                starTreeDocument.metrics[0]
            );
        }
        validateStarTree(builder.getRootNode(), 2, 1, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.SUM)),
            100,
            1,
            Set.of(),
            getBuildMode(),
            0,
            6715
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    private static DocValuesProducer getDocValuesProducer(SortedNumericDocValues sndv) {
        return new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                return sndv;
            }
        };
    }

    private StarTreeField getStarTreeFieldWithMultipleMetrics() {
        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        Metric m2 = new Metric("field2", List.of(MetricStat.COUNT));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1, m2);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1000, new HashSet<>(), getBuildMode());
        return new StarTreeField("sf", dims, metrics, c);
    }

    public void testMergeFlowWithSum() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(
            getLongFromDouble(0.0),
            getLongFromDouble(10.0),
            getLongFromDouble(20.0),
            getLongFromDouble(30.0),
            getLongFromDouble(40.0),
            getLongFromDouble(50.0),
            getLongFromDouble(60.0)

        );
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        StarTreeField sf = getStarTreeField(MetricStat.SUM);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(6), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [ metric] ]
         * [0, 0] | [0.0]
         * [1, 1] | [20.0]
         * [3, 3] | [60.0]
         * [4, 4] | [80.0]
         * [5, 5] | [100.0]
         * [null, 2] | [40.0]
         * ------------------ We only take non star docs
         * [6,-1] | [120.0]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 6) {
                assertEquals(
                    starTreeDocument.dimensions[0] != null ? starTreeDocument.dimensions[0] * 2 * 10.0 : 40.0,
                    starTreeDocument.metrics[0]
                );
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.SUM)),
            6,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            280
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowWithCount() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(6), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [2]
         [3, 3] | [6]
         [4, 4] | [8]
         [5, 5] | [10]
         [null, 2] | [4]
         ---------------
         [6,-1] | [12]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 6) {
                assertEquals(starTreeDocument.dimensions[0] != null ? starTreeDocument.dimensions[0] * 2 : 4, starTreeDocument.metrics[0]);
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.COUNT)),
            6,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            280
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);

    }

    private StarTreeValues getStarTreeValues(
        SortedNumericDocValues dimList,
        SortedNumericDocValues dimList2,
        SortedNumericDocValues metricsList,
        StarTreeField sf,
        String number
    ) {
        SortedNumericDocValues d1sndv = dimList;
        SortedNumericDocValues d2sndv = dimList2;
        SortedNumericDocValues m1sndv = metricsList;
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            Map.of(CompositeIndexConstants.SEGMENT_DOCS_COUNT, number)
        );
        return starTreeValues;
    }

    public void testMergeFlowWithDifferentDocsFromSegments() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            sf,
            "4"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(4), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [10]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 2] | [2]
         [null, 7] | [7]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 9) {
                if (Objects.equals(starTreeDocument.dimensions[0], 5L)) {
                    assertEquals(starTreeDocument.dimensions[0] * 2, starTreeDocument.metrics[0]);
                } else {
                    assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
                }
            }
        }
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.COUNT)),
            9,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            346
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowNumSegmentsDocs() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, -1L, -1L, -1L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, -1L, -1L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, -1L, -1L, -1L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Long> dimList3 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 2, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            sf,
            "4"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(4), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [10]
         [6, 6] | [6]
         [7, 7] | [7]
         [8, 8] | [8]
         */
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            if (Objects.equals(starTreeDocument.dimensions[0], 5L)) {
                assertEquals(starTreeDocument.dimensions[0] * 2, starTreeDocument.metrics[0]);
            } else {
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }
        assertEquals(9, count);
    }

    public void testMergeFlowWithMissingDocs() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            sf,
            "4"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(4), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 5] | [5]
         [null, 7] | [7]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 10) {
                if (starTreeDocument.dimensions[0] == null) {
                    assertTrue(List.of(5L, 7L).contains(starTreeDocument.dimensions[1]));
                }
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.COUNT)),
            10,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            379
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowWithMissingDocsWithZero() throws IOException {
        List<Long> dimList = List.of(0L, 0L, 0L, 0L);
        List<Integer> docsWithField = List.of(0, 1, 2, 6);
        List<Long> dimList2 = List.of(0L, 0L, 0L, 0L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "7"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            sf,
            "4"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(4), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [9]
         [5, 5] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 7] | [7]
         [null, null] | [12]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 6) {
                if (starTreeDocument.dimensions[0] == null && starTreeDocument.dimensions[1] == null) {
                    assertEquals(12L, (long) starTreeDocument.metrics[0]);
                } else if (starTreeDocument.dimensions[0] == null) {
                    assertEquals(7L, starTreeDocument.metrics[0]);
                } else if (starTreeDocument.dimensions[0] == 0) {
                    assertEquals(9L, starTreeDocument.metrics[0]);
                } else {
                    assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
                }
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.COUNT)),
            6,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            247
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowWithMissingDocsWithZeroComplexCase() throws IOException {
        List<Long> dimList = List.of(0L, 0L, 0L, 0L, 0L);
        List<Integer> docsWithField = List.of(0, 1, 2, 6, 8);
        List<Long> dimList2 = List.of(0L, 0L, 0L, 0L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "9"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            sf,
            "4"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(4), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [9]
         [0, null] | [8]
         [5, 5] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 7] | [7]
         [null, null] | [19]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 7) {
                if (starTreeDocument.dimensions[0] == null && starTreeDocument.dimensions[1] == null) {
                    assertEquals(19L, (long) starTreeDocument.metrics[0]);
                    assertEquals(7, count);
                } else if (starTreeDocument.dimensions[0] == null) {
                    assertEquals(7L, starTreeDocument.metrics[0]);
                } else if (starTreeDocument.dimensions[1] == null) {
                    assertEquals(8L, starTreeDocument.metrics[0]);
                } else if (starTreeDocument.dimensions[0] == 0) {
                    assertEquals(9L, starTreeDocument.metrics[0]);
                } else {
                    assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
                }
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.COUNT)),
            7,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            247
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowWithMissingDocsInSecondDim() throws IOException {
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 6L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 6);
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            sf,
            "4"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(4), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [5]
         [5, null] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 7] | [7]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 10) {
                if (starTreeDocument.dimensions[0] != null && starTreeDocument.dimensions[0] == 5) {
                    assertEquals(starTreeDocument.dimensions[0], starTreeDocument.metrics[0]);
                } else {
                    assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
                }
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.COUNT)),
            10,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            379
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowWithDocsMissingAtTheEnd() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            sf,
            "4"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 5] | [5]
         [null, 7] | [7]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 10) {
                if (starTreeDocument.dimensions[0] == null) {
                    assertTrue(List.of(5L, 7L).contains(starTreeDocument.dimensions[1]));
                }
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.COUNT)),
            10,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            379
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowWithEmptyFieldsInOneSegment() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            DocValues.emptySortedNumeric(),
            DocValues.emptySortedNumeric(),
            DocValues.emptySortedNumeric(),
            sf,
            "0"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(0), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [null, 5] | [5]
         */
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 6) {
                if (starTreeDocument.dimensions[0] == null) {
                    assertEquals(5L, (long) starTreeDocument.dimensions[1]);
                }
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3"),
            List.of(new MetricEntry("field2", MetricStat.COUNT)),
            6,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            280
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowWithDuplicateDimensionValues() throws IOException {
        List<Long> dimList1 = new ArrayList<>(500);
        List<Integer> docsWithField1 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 5 + j);
            }
        }

        List<Long> dimList2 = new ArrayList<>(500);
        List<Integer> docsWithField2 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList2.add((long) i);
                docsWithField2.add(i * 5 + j);
            }
        }

        List<Long> dimList3 = new ArrayList<>(500);
        List<Integer> docsWithField3 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList3.add((long) i);
                docsWithField3.add(i * 5 + j);
            }
        }

        List<Long> dimList4 = new ArrayList<>(500);
        List<Integer> docsWithField4 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList4.add((long) i);
                docsWithField4.add(i * 5 + j);
            }
        }

        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        StarTreeField sf = getStarTreeField(1);
        StarTreeValues starTreeValues = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            sf
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            sf
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, writeState, mapperService);
        builder.build(builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2)), new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(401, starTreeDocuments.size());
        int count = 0;
        double sum = 0;
        /**
         401 docs get generated
         [0, 0, 0, 0] | [200.0]
         [1, 1, 1, 1] | [700.0]
         [2, 2, 2, 2] | [1200.0]
         [3, 3, 3, 3] | [1700.0]
         [4, 4, 4, 4] | [2200.0]
         .....
         [null, null, null, 99] | [49700.0]
         [null, null, null, null] | [2495000.0]
         */
        for (StarTreeDocument starTreeDocument : starTreeDocuments) {
            if (starTreeDocument.dimensions[3] == null) {
                assertEquals(sum, starTreeDocument.metrics[0]);
            } else {
                if (starTreeDocument.dimensions[0] != null) {
                    sum += (double) starTreeDocument.metrics[0];
                }
                assertEquals(starTreeDocument.dimensions[3] * 500 + 200.0, starTreeDocument.metrics[0]);
            }
            count++;
        }
        assertEquals(401, count);
        validateStarTree(builder.getRootNode(), 4, sf.getStarTreeConfig().maxLeafDocs(), builder.getStarTreeDocuments());
        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(new MetricEntry("field2", MetricStat.SUM)),
            100,
            1,
            Set.of(),
            getBuildMode(),
            0,
            13381
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public void testMergeFlowWithMaxLeafDocs() throws IOException {
        List<Long> dimList1 = new ArrayList<>(500);
        List<Integer> docsWithField1 = new ArrayList<>(500);

        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 20 + j);
            }
        }
        for (int i = 80; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 5 + j);
            }
        }
        List<Long> dimList3 = new ArrayList<>(500);
        List<Integer> docsWithField3 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList3.add((long) i);
                docsWithField3.add(i * 5 + j);
            }
        }
        List<Long> dimList2 = new ArrayList<>(500);
        List<Integer> docsWithField2 = new ArrayList<>(500);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 50; j++) {
                dimList2.add((long) i);
                docsWithField2.add(i * 50 + j);
            }
        }

        List<Long> dimList4 = new ArrayList<>(500);
        List<Integer> docsWithField4 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList4.add((long) i);
                docsWithField4.add(i * 5 + j);
            }
        }

        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        StarTreeField sf = getStarTreeField(3);
        StarTreeValues starTreeValues = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            sf
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            sf
        );

        builder = getStarTreeBuilder(metaOut, dataOut, sf, writeState, mapperService);
        builder.build(builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2)), new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        /**
         635 docs get generated
         [0, 0, 0, 0] | [200.0]
         [1, 1, 1, 1] | [700.0]
         [2, 2, 2, 2] | [1200.0]
         [3, 3, 3, 3] | [1700.0]
         [4, 4, 4, 4] | [2200.0]
         .....
         [null, null, null, 99] | [49700.0]
         .....
         [null, null, null, null] | [2495000.0]
         */
        assertEquals(635, starTreeDocuments.size());
        validateStarTree(builder.getRootNode(), 4, sf.getStarTreeConfig().maxLeafDocs(), builder.getStarTreeDocuments());
        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(new MetricEntry("field2", MetricStat.SUM)),
            100,
            3,
            Set.of(),
            getBuildMode(),
            0,
            23215
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    private StarTreeValues getStarTreeValues(
        List<Long> dimList1,
        List<Integer> docsWithField1,
        List<Long> dimList2,
        List<Integer> docsWithField2,
        List<Long> dimList3,
        List<Integer> docsWithField3,
        List<Long> dimList4,
        List<Integer> docsWithField4,
        List<Long> metricsList,
        List<Integer> metricsWithField,
        StarTreeField sf
    ) {
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList1, docsWithField1);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues d3sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues d4sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv, "field5", d3sndv, "field8", d4sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(sf, null, dimDocIdSetIterators, metricDocIdSetIterators, getAttributes(500));
        return starTreeValues;
    }

    public void testMergeFlowWithDuplicateDimensionValueWithMaxLeafDocs() throws IOException {
        List<Long> dimList1 = new ArrayList<>(500);
        List<Integer> docsWithField1 = new ArrayList<>(500);

        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 20 + j);
            }
        }
        for (int i = 80; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 5 + j);
            }
        }
        List<Long> dimList3 = new ArrayList<>(500);
        List<Integer> docsWithField3 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList3.add((long) i);
                docsWithField3.add(i * 5 + j);
            }
        }
        List<Long> dimList2 = new ArrayList<>(500);
        List<Integer> docsWithField2 = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dimList2.add((long) 1);
            docsWithField2.add(i);
        }

        List<Long> dimList4 = new ArrayList<>(500);
        List<Integer> docsWithField4 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList4.add((long) i);
                docsWithField4.add(i * 5 + j);
            }
        }

        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        StarTreeField sf = getStarTreeField(3);
        StarTreeValues starTreeValues = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            sf
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            sf
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, writeState, mapperService);
        builder.build(builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2)), new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(401, starTreeDocuments.size());
        validateStarTree(builder.getRootNode(), 4, sf.getStarTreeConfig().maxLeafDocs(), builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(new MetricEntry("field2", MetricStat.SUM)),
            100,
            sf.getStarTreeConfig().maxLeafDocs(),
            Set.of(),
            getBuildMode(),
            0,
            15361
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    public static long getLongFromDouble(double value) {
        return Double.doubleToLongBits(value);
    }

    public void testMergeFlowWithMaxLeafDocsAndStarTreeNodesAssertion() throws IOException {
        List<Long> dimList1 = new ArrayList<>(500);
        List<Integer> docsWithField1 = new ArrayList<>(500);
        Map<Integer, Map<Long, Double>> expectedDimToValueMap = new HashMap<>();
        Map<Long, Double> dimValueMap = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 20 + j);
            }
            // metric = no of docs * 10.0
            dimValueMap.put((long) i, 200.0);
        }
        for (int i = 80; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 5 + j);
            }
            // metric = no of docs * 10.0
            dimValueMap.put((long) i, 50.0);
        }
        dimValueMap.put(Long.MAX_VALUE, 5000.0);
        expectedDimToValueMap.put(0, dimValueMap);
        dimValueMap = new HashMap<>();
        List<Long> dimList3 = new ArrayList<>(500);
        List<Integer> docsWithField3 = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dimList3.add((long) 1);
            docsWithField3.add(i);
            dimValueMap.put((long) i, 10.0);
        }
        dimValueMap.put(Long.MAX_VALUE, 5000.0);
        expectedDimToValueMap.put(2, dimValueMap);
        dimValueMap = new HashMap<>();
        List<Long> dimList2 = new ArrayList<>(500);
        List<Integer> docsWithField2 = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dimList2.add((long) i);
            docsWithField2.add(i);
            dimValueMap.put((long) i, 10.0);
        }
        dimValueMap.put(Long.MAX_VALUE, 200.0);
        expectedDimToValueMap.put(1, dimValueMap);
        dimValueMap = new HashMap<>();
        List<Long> dimList4 = new ArrayList<>(500);
        List<Integer> docsWithField4 = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dimList4.add((long) 1);
            docsWithField4.add(i);
            dimValueMap.put((long) i, 10.0);
        }
        dimValueMap.put(Long.MAX_VALUE, 5000.0);
        expectedDimToValueMap.put(3, dimValueMap);
        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(10.0));
            metricsWithField.add(i);
        }

        StarTreeField sf = getStarTreeField(10);
        StarTreeValues starTreeValues = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            sf
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            sf
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, writeState, mapperService);
        builder.build(builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2)), new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        Map<Integer, Map<Long, Integer>> dimValueToDocIdMap = new HashMap<>();
        traverseStarTree(builder.rootNode, dimValueToDocIdMap, true);
        for (Map.Entry<Integer, Map<Long, Integer>> entry : dimValueToDocIdMap.entrySet()) {
            int dimId = entry.getKey();
            if (dimId == -1) continue;
            Map<Long, Double> map = expectedDimToValueMap.get(dimId);
            for (Map.Entry<Long, Integer> dimValueToDocIdEntry : entry.getValue().entrySet()) {
                long dimValue = dimValueToDocIdEntry.getKey();
                int docId = dimValueToDocIdEntry.getValue();
                assertEquals(map.get(dimValue) * 2, starTreeDocuments.get(docId).metrics[0]);
            }
        }
        assertEquals(1041, starTreeDocuments.size());
        validateStarTree(builder.getRootNode(), 4, sf.getStarTreeConfig().maxLeafDocs(), builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(new MetricEntry("field2", MetricStat.SUM)),
            500,
            sf.getStarTreeConfig().maxLeafDocs(),
            Set.of(),
            getBuildMode(),
            0,
            31795
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    private StarTreeField getStarTreeField(int maxLeafDocs) {
        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Dimension d3 = new NumericDimension("field5");
        Dimension d4 = new NumericDimension("field8");
        List<Dimension> dims = List.of(d1, d2, d3, d4);
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        List<Metric> metrics = List.of(m1);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(maxLeafDocs, new HashSet<>(), getBuildMode());
        StarTreeField sf = new StarTreeField("sf", dims, metrics, c);
        return sf;
    }

    private void traverseStarTree(InMemoryTreeNode root, Map<Integer, Map<Long, Integer>> dimValueToDocIdMap, boolean traverStarNodes) {
        InMemoryTreeNode starTree = root;
        // Use BFS to traverse the star tree
        Queue<InMemoryTreeNode> queue = new ArrayDeque<>();
        queue.add(starTree);
        int currentDimensionId = -1;
        InMemoryTreeNode starTreeNode;
        List<Integer> docIds = new ArrayList<>();
        while ((starTreeNode = queue.poll()) != null) {
            int dimensionId = starTreeNode.dimensionId;
            if (dimensionId > currentDimensionId) {
                currentDimensionId = dimensionId;
            }

            // store aggregated document of the node
            int docId = starTreeNode.aggregatedDocId;
            Map<Long, Integer> map = dimValueToDocIdMap.getOrDefault(dimensionId, new HashMap<>());
            if (starTreeNode.nodeType == StarTreeNodeType.STAR.getValue()) {
                map.put(Long.MAX_VALUE, docId);
            } else {
                map.put(starTreeNode.dimensionValue, docId);
            }
            dimValueToDocIdMap.put(dimensionId, map);

            if (starTreeNode.children != null && (!traverStarNodes || (starTreeNode.nodeType == StarTreeNodeType.STAR.getValue()))) {
                Iterator<InMemoryTreeNode> childrenIterator = starTreeNode.children.values().iterator();
                while (childrenIterator.hasNext()) {
                    InMemoryTreeNode childNode = childrenIterator.next();
                    queue.add(childNode);
                }
            }
        }
    }

    public void testMergeFlow() throws IOException {
        List<Long> dimList1 = new ArrayList<>(1000);
        List<Integer> docsWithField1 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList1.add((long) i);
            docsWithField1.add(i);
        }

        List<Long> dimList2 = new ArrayList<>(1000);
        List<Integer> docsWithField2 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList2.add((long) i);
            docsWithField2.add(i);
        }

        List<Long> dimList3 = new ArrayList<>(1000);
        List<Integer> docsWithField3 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList3.add((long) i);
            docsWithField3.add(i);
        }

        List<Long> dimList4 = new ArrayList<>(1000);
        List<Integer> docsWithField4 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList4.add((long) i);
            docsWithField4.add(i);
        }

        List<Long> dimList5 = new ArrayList<>(1000);
        List<Integer> docsWithField5 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList5.add((long) i);
            docsWithField5.add(i);
        }

        List<Long> metricsList = new ArrayList<>(1000);
        List<Integer> metricsWithField = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Dimension d3 = new NumericDimension("field5");
        Dimension d4 = new NumericDimension("field8");
        // Dimension d5 = new NumericDimension("field5");
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        List<Dimension> dims = List.of(d1, d2, d3, d4);
        List<Metric> metrics = List.of(m1);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1, new HashSet<>(), getBuildMode());
        StarTreeField sf = new StarTreeField("sf", dims, metrics, c);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList1, docsWithField1);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues d3sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues d4sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv, "field5", d3sndv, "field8", d4sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(sf, null, dimDocIdSetIterators, metricDocIdSetIterators, getAttributes(1000));

        SortedNumericDocValues f2d1sndv = getSortedNumericMock(dimList1, docsWithField1);
        SortedNumericDocValues f2d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues f2d3sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues f2d4sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues f2m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> f2dimDocIdSetIterators = Map.of(
            "field1",
            f2d1sndv,
            "field3",
            f2d2sndv,
            "field5",
            f2d3sndv,
            "field8",
            f2d4sndv
        );
        Map<String, DocIdSetIterator> f2metricDocIdSetIterators = Map.of("field2", f2m1sndv);
        StarTreeValues starTreeValues2 = new StarTreeValues(
            sf,
            null,
            f2dimDocIdSetIterators,
            f2metricDocIdSetIterators,
            getAttributes(1000)
        );

        builder = getStarTreeBuilder(metaOut, dataOut, sf, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         [0, 0, 0, 0] | [0.0]
         [1, 1, 1, 1] | [20.0]
         [2, 2, 2, 2] | [40.0]
         [3, 3, 3, 3] | [60.0]
         [4, 4, 4, 4] | [80.0]
         [5, 5, 5, 5] | [100.0]
         ...
         [999, 999, 999, 999] | [19980.0]
         */
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            assertEquals(starTreeDocument.dimensions[0] * 20.0, starTreeDocument.metrics[0]);
        }
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);

        // Validate the star tree structure
        validateStarTree(builder.getRootNode(), 4, 1, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            List.of("field1", "field3", "field5", "field8"),
            List.of(new MetricEntry("field2", MetricStat.SUM)),
            1000,
            sf.getStarTreeConfig().maxLeafDocs(),
            Set.of(),
            getBuildMode(),
            0,
            132181
        );

        validateStarTreeFileFormats(builder.getRootNode(), 3, starTreeMetadata);
    }

    private void validateStarTree(
        InMemoryTreeNode root,
        int totalDimensions,
        int maxLeafDocuments,
        List<StarTreeDocument> starTreeDocuments
    ) {
        Queue<Object[]> queue = new LinkedList<>();
        queue.offer(new Object[] { root, false });
        while (!queue.isEmpty()) {
            Object[] current = queue.poll();
            InMemoryTreeNode node = (InMemoryTreeNode) current[0];
            boolean currentIsStarNode = (boolean) current[1];

            assertNotNull(node);

            // assert dimensions
            if (node.dimensionId != ALL) {
                assertTrue(node.dimensionId >= 0 && node.dimensionId < totalDimensions);
            }

            if (node.children != null && !node.children.isEmpty()) {
                assertEquals(node.dimensionId + 1, node.childDimensionId);
                assertTrue(node.childDimensionId < totalDimensions);
                InMemoryTreeNode starNode = null;
                Object[] nonStarNodeCumulativeMetrics = getMetrics(starTreeDocuments);
                for (Map.Entry<Long, InMemoryTreeNode> entry : node.children.entrySet()) {
                    Long childDimensionValue = entry.getKey();
                    InMemoryTreeNode child = entry.getValue();
                    Object[] currMetrics = getMetrics(starTreeDocuments);
                    if (child.nodeType != StarTreeNodeType.STAR.getValue()) {
                        // Validate dimension values in documents
                        for (int i = child.startDocId; i < child.endDocId; i++) {
                            StarTreeDocument doc = starTreeDocuments.get(i);
                            int j = 0;
                            addMetrics(doc, currMetrics, j);
                            if (child.nodeType != StarTreeNodeType.STAR.getValue()) {
                                Long dimension = doc.dimensions[child.dimensionId];
                                assertEquals(childDimensionValue, dimension);
                                if (dimension != null) {
                                    assertEquals(child.dimensionValue, (long) dimension);
                                } else {
                                    // TODO : fix this ?
                                    assertEquals(child.dimensionValue, ALL);
                                }
                            }
                        }
                        Object[] aggregatedMetrics = starTreeDocuments.get(child.aggregatedDocId).metrics;
                        int j = 0;
                        for (Object metric : currMetrics) {
                            /*
                             * TODO : refactor this to handle any data type
                             */
                            if (metric instanceof Double) {
                                nonStarNodeCumulativeMetrics[j] = (double) nonStarNodeCumulativeMetrics[j] + (double) metric;
                                assertEquals((Double) metric, (Double) aggregatedMetrics[j], 0);
                            } else if (metric instanceof Long) {
                                nonStarNodeCumulativeMetrics[j] = (long) nonStarNodeCumulativeMetrics[j] + (long) metric;
                                assertEquals((long) metric, (long) aggregatedMetrics[j]);
                            } else if (metric instanceof Float) {
                                nonStarNodeCumulativeMetrics[j] = (float) nonStarNodeCumulativeMetrics[j] + (float) metric;
                                assertEquals((float) metric, (float) aggregatedMetrics[j], 0);
                            }
                            j++;
                        }
                        queue.offer(new Object[] { child, false });
                    } else {
                        starNode = child;
                    }
                }
                // Add star node to queue
                if (starNode != null) {
                    Object[] starNodeMetrics = getMetrics(starTreeDocuments);
                    for (int i = starNode.startDocId; i < starNode.endDocId; i++) {
                        StarTreeDocument doc = starTreeDocuments.get(i);
                        int j = 0;
                        addMetrics(doc, starNodeMetrics, j);
                    }
                    int j = 0;
                    Object[] aggregatedMetrics = starTreeDocuments.get(starNode.aggregatedDocId).metrics;
                    for (Object nonStarNodeCumulativeMetric : nonStarNodeCumulativeMetrics) {
                        assertEquals(nonStarNodeCumulativeMetric, starNodeMetrics[j]);
                        assertEquals(starNodeMetrics[j], aggregatedMetrics[j]);
                        /*
                         * TODO : refactor this to handle any data type
                         */
                        if (nonStarNodeCumulativeMetric instanceof Double) {
                            assertEquals((double) nonStarNodeCumulativeMetric, (double) starNodeMetrics[j], 0);
                            assertEquals((double) nonStarNodeCumulativeMetric, (double) aggregatedMetrics[j], 0);
                        } else if (nonStarNodeCumulativeMetric instanceof Long) {
                            assertEquals((long) nonStarNodeCumulativeMetric, (long) starNodeMetrics[j]);
                            assertEquals((long) nonStarNodeCumulativeMetric, (long) aggregatedMetrics[j]);
                        } else if (nonStarNodeCumulativeMetric instanceof Float) {
                            assertEquals((float) nonStarNodeCumulativeMetric, (float) starNodeMetrics[j], 0);
                            assertEquals((float) nonStarNodeCumulativeMetric, (float) aggregatedMetrics[j], 0);
                        }

                        j++;
                    }
                    assertEquals(-1L, starNode.dimensionValue);
                    queue.offer(new Object[] { starNode, true });
                }
            } else {
                assertTrue(node.endDocId - node.startDocId <= maxLeafDocuments);
            }

            if (currentIsStarNode) {
                StarTreeDocument prevDoc = null;
                int docCount = 0;
                int docId = node.startDocId;
                int dimensionId = node.dimensionId;

                while (docId < node.endDocId) {
                    StarTreeDocument currentDoc = starTreeDocuments.get(docId);
                    docCount++;

                    // Verify that the dimension at 'dimensionId' is set to STAR_IN_DOC_VALUES_INDEX
                    assertNull(currentDoc.dimensions[dimensionId]);

                    // Verify sorting of documents
                    if (prevDoc != null) {
                        assertTrue(compareDocuments(prevDoc, currentDoc, dimensionId + 1, totalDimensions) <= 0);
                    }
                    prevDoc = currentDoc;
                    docId++;
                }

                // Verify that the number of generated star documents matches the range in the star node
                assertEquals(node.endDocId - node.startDocId, docCount);
            }
        }
    }

    /**
     * TODO : refactor this to handle any data type
     */
    private static void addMetrics(StarTreeDocument doc, Object[] currMetrics, int j) {
        for (Object metric : doc.metrics) {
            if (metric instanceof Double) {
                currMetrics[j] = (double) currMetrics[j] + (double) metric;
            } else if (metric instanceof Long) {
                currMetrics[j] = (long) currMetrics[j] + (long) metric;
            } else if (metric instanceof Float) {
                currMetrics[j] = (float) currMetrics[j] + (float) metric;
            }
            j++;
        }
    }

    private static Object[] getMetrics(List<StarTreeDocument> starTreeDocuments) {
        Object[] nonStarNodeCumulativeMetrics = new Object[starTreeDocuments.get(0).metrics.length];
        for (int i = 0; i < nonStarNodeCumulativeMetrics.length; i++) {
            if (starTreeDocuments.get(0).metrics[i] instanceof Long) {
                nonStarNodeCumulativeMetrics[i] = 0L;
            } else if (starTreeDocuments.get(0).metrics[i] instanceof Double) {
                nonStarNodeCumulativeMetrics[i] = 0.0;
            } else if (starTreeDocuments.get(0).metrics[i] instanceof Float) {
                nonStarNodeCumulativeMetrics[i] = 0.0f;
            }
        }
        return nonStarNodeCumulativeMetrics;
    }

    private int compareDocuments(StarTreeDocument doc1, StarTreeDocument doc2, int startDim, int endDim) {
        for (int i = startDim; i < endDim; i++) {
            Long val1 = doc1.dimensions[i];
            Long val2 = doc2.dimensions[i];

            if (!Objects.equals(val1, val2)) {
                if (val1 == null) return 1;
                if (val2 == null) return -1;
                return Long.compare(val1, val2);
            }
        }
        return 0;
    }

    Map<String, String> getAttributes(int numSegmentDocs) {
        return Map.of(CompositeIndexConstants.SEGMENT_DOCS_COUNT, String.valueOf(numSegmentDocs));
    }

    private StarTreeField getStarTreeField(MetricStat count) {
        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Metric m1 = new Metric("field2", List.of(count));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1000, new HashSet<>(), getBuildMode());
        return new StarTreeField("sf", dims, metrics, c);
    }

    private Long getLongFromDouble(Double num) {
        if (num == null) {
            return null;
        }
        return NumericUtils.doubleToSortableLong(num);
    }

    SortedNumericDocValues getSortedNumericMock(List<Long> dimList, List<Integer> docsWithField) {
        return new SortedNumericDocValues() {
            int index = -1;

            @Override
            public long nextValue() {
                return dimList.get(index);
            }

            @Override
            public int docValueCount() {
                return 0;
            }

            @Override
            public boolean advanceExact(int target) {
                return false;
            }

            @Override
            public int docID() {
                return index;
            }

            @Override
            public int nextDoc() {
                if (index == docsWithField.size() - 1) {
                    return NO_MORE_DOCS;
                }
                index++;
                return docsWithField.get(index);
            }

            @Override
            public int advance(int target) {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (builder != null) {
            builder.close();
        }
        docValuesConsumer.close();
        metaOut.close();
        dataOut.close();
        directory.close();
    }
}

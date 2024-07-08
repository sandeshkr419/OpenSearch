/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.startree.node.OffHeapStarTree;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reader for star tree index and star tree doc values from the segments
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite90DocValuesReader extends DocValuesProducer implements CompositeIndexReader {
    private static final Logger logger = LogManager.getLogger(CompositeIndexMetadata.class);

    private final DocValuesProducer delegate;
    private final IndexInput dataIn;
    private final ChecksumIndexInput metaIn;
    private final Map<String, StarTree> starTreeMap = new LinkedHashMap<>();
    private final Map<String, CompositeIndexMetadata> starTreeMetaMap = new LinkedHashMap<>();
    private final List<CompositeIndexFieldInfo> compositeFieldInfos = new ArrayList<>();

    public Composite90DocValuesReader(DocValuesProducer producer, SegmentReadState readState) throws IOException {
        this.delegate = producer;

        String metaFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Composite90DocValuesFormat.META_EXTENSION
        );

        String dataFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Composite90DocValuesFormat.DATA_EXTENSION
        );

        boolean success = false;
        try {

            dataIn = readState.directory.openInput(dataFileName, readState.context);
            CodecUtil.checkIndexHeader(
                dataIn,
                Composite90DocValuesFormat.DATA_CODEC_NAME,
                Composite90DocValuesFormat.VERSION_START,
                Composite90DocValuesFormat.VERSION_CURRENT,
                readState.segmentInfo.getId(),
                readState.segmentSuffix
            );
            CodecUtil.retrieveChecksum(dataIn);

            metaIn = readState.directory.openChecksumInput(metaFileName, readState.context);
            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(
                    metaIn,
                    Composite90DocValuesFormat.META_CODEC_NAME,
                    Composite90DocValuesFormat.VERSION_START,
                    Composite90DocValuesFormat.VERSION_CURRENT,
                    readState.segmentInfo.getId(),
                    readState.segmentSuffix
                );

                while (true) {
                    long magicMarker = metaIn.readLong();

                    if (magicMarker == -1) {
                        logger.info("EOF reached for composite index metadata");
                        return;
                    } else if (magicMarker < 0) {
                        throw new CorruptIndexException("Unknown token encountered: " + magicMarker, metaIn);
                    }
                    CompositeIndexMetadata compositeIndexMetadata = new CompositeIndexMetadata(metaIn, magicMarker);
                    compositeFieldInfos.add(
                        new CompositeIndexFieldInfo(
                            compositeIndexMetadata.getCompositeFieldName(),
                            compositeIndexMetadata.getCompositeFieldType()
                        )
                    );
                    switch (compositeIndexMetadata.getCompositeFieldType()) {
                        case STAR_TREE:
                            StarTree starTree = new OffHeapStarTree(dataIn, compositeIndexMetadata.getStarTreeMetadata());
                            starTreeMap.put(compositeIndexMetadata.getCompositeFieldName(), starTree);
                            starTreeMetaMap.put(compositeIndexMetadata.getCompositeFieldName(), compositeIndexMetadata);
                            break;
                        default:
                            throw new CorruptIndexException("Invalid composite field type found in the file", dataIn);
                    }
                }
            } catch (Throwable t) {
                priorE = t;
            } finally {
                CodecUtil.checkFooter(metaIn, priorE);
            }
            CodecUtil.retrieveChecksum(dataIn);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }

    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return delegate.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return delegate.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
        CodecUtil.checksumEntireFile(metaIn);
        CodecUtil.checksumEntireFile(dataIn);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        starTreeMap.clear();
        starTreeMetaMap.clear();
    }

    @Override
    public List<CompositeIndexFieldInfo> getCompositeIndexFields() {
        return compositeFieldInfos;

    }

    @Override
    public CompositeIndexValues getCompositeIndexValues(CompositeIndexFieldInfo compositeIndexFieldInfo) throws IOException {
        // TODO : read compositeIndexValues [starTreeValues] from star tree files

        throw new UnsupportedOperationException();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Query;
import org.opensearch.be.lucene.predicate.QueryBuilderSerializer;
import org.opensearch.be.lucene.predicate.RexToQueryBuilderConverter;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Core Lucene filter execution logic.
 * <p>
 * Planner side: {@link #convertFragment} converts RexNode → QueryBuilder → byte[].
 * Executor side: {@link #execute} and {@link #executeForSegment} delegate to
 * {@link LuceneIndexFilterProvider} for segment-level scoring.
 * Results are packaged as Arrow BitVector.
 * <p>
 * Lifecycle: call {@link #initialize} with a DirectoryReader and QueryShardContext,
 * then call execute methods, then {@link #close} to release resources.
 */
public class LuceneFilterExecutor implements Closeable {

    /** Column name for the document ID bitset in the result schema. */
    public static final String DOC_IDS_COLUMN = "doc_ids";

    private static final Schema DOC_IDS_SCHEMA = new Schema(
        List.of(Field.nullable(DOC_IDS_COLUMN, new ArrowType.Bool()))
    );

    private DirectoryReader directoryReader;
    private QueryShardContext queryShardContext;
    private BufferAllocator allocator;

    private final LuceneIndexFilterProvider filterProvider = new LuceneIndexFilterProvider();
    private final Map<ByteBuffer, LuceneIndexFilterContext> contextCache = new HashMap<>();
    private final Map<Long, Integer> collectorCache = new HashMap<>();

    /**
     * Initializes with a DirectoryReader and QueryShardContext for execution.
     */
    public void initialize(DirectoryReader reader, QueryShardContext qsc) {
        this.directoryReader = reader;
        this.queryShardContext = qsc;
    }

    @Override
    public void close() {
        for (LuceneIndexFilterContext ctx : contextCache.values()) {
            ctx.close();
        }
        collectorCache.clear();
        contextCache.clear();
        directoryReader = null;
        queryShardContext = null;
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    private BufferAllocator getAllocator() {
        if (allocator == null) {
            allocator = new RootAllocator();
        }
        return allocator;
    }

    private LuceneIndexFilterContext getOrCreateContext(byte[] fragment) throws IOException {
        ByteBuffer key = ByteBuffer.wrap(fragment);
        LuceneIndexFilterContext cached = contextCache.get(key);
        if (cached != null) {
            return cached;
        }
        QueryBuilder queryBuilder = QueryBuilderSerializer.deserialize(fragment);
        Query query = queryBuilder.toQuery(queryShardContext);
        LuceneIndexFilterContext ctx = filterProvider.createContext(query, directoryReader);
        contextCache.put(key, ctx);
        return ctx;
    }

    private static long collectorCacheKey(ByteBuffer fragmentKey, int segmentOrd) {
        return ((long) fragmentKey.hashCode() << 32) | (segmentOrd & 0xFFFFFFFFL);
    }

    // --- Planner side ---

    /**
     * Converts a RelNode fragment into serialized QueryBuilder bytes.
     * Stateless — does not require initialization.
     */
    public byte[] convertFragment(RelNode fragment) {
        return convertFragment(fragment, null);
    }

    public byte[] convertFragment(RelNode fragment, MapperService mapperService) {
        Objects.requireNonNull(fragment, "RelNode fragment must not be null");
        if (!(fragment instanceof LogicalFilter)) {
            throw new IllegalArgumentException(
                "Lucene backend expects a LogicalFilter, got: " + fragment.getClass().getSimpleName()
            );
        }
        LogicalFilter filter = (LogicalFilter) fragment;
        RexNode condition = filter.getCondition();
        RelDataType inputRowType = filter.getInput().getRowType();
        RexToQueryBuilderConverter converter = new RexToQueryBuilderConverter(inputRowType, mapperService);
        QueryBuilder queryBuilder = converter.convert(condition);
        return QueryBuilderSerializer.serialize(queryBuilder);
    }

    // --- Executor side ---

    /**
     * Executes across all segments. Requires initialization.
     */
    public Iterator<VectorSchemaRoot> execute(byte[] fragment) {
        if (fragment == null || fragment.length == 0) {
            throw new IllegalArgumentException("Fragment byte array must not be null or empty");
        }
        QueryBuilderSerializer.deserialize(fragment); // validate
        if (directoryReader == null) {
            return createEmptyResult();
        }
        return executeAllSegments(fragment);
    }

    /**
     * Executes for a specific segment and doc ID range.
     * Reuses Scorer across sequential batch calls within the same segment.
     */
    public Iterator<VectorSchemaRoot> executeForSegment(byte[] fragment, int segmentOrd, int startDocId, int endDocId) {
        if (fragment == null || fragment.length == 0) {
            throw new IllegalArgumentException("Fragment byte array must not be null or empty");
        }
        if (directoryReader == null) {
            return createEmptyResult();
        }
        try {
            LuceneIndexFilterContext ctx = getOrCreateContext(fragment);
            ByteBuffer fragmentKey = ByteBuffer.wrap(fragment);
            long cacheKey = collectorCacheKey(fragmentKey, segmentOrd);

            Integer collectorKey = collectorCache.get(cacheKey);
            if (collectorKey == null) {
                collectorKey = filterProvider.createCollector(ctx, segmentOrd, startDocId, endDocId);
                collectorCache.put(cacheKey, collectorKey);
            }

            long[] bits = filterProvider.collectDocs(ctx, collectorKey, startDocId, endDocId);
            return createResultFromLongArray(bits, endDocId - startDocId);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed during Lucene segment query execution: " + e.getMessage(), e);
        }
    }

    public int getSegmentCount() {
        if (directoryReader == null) return 0;
        return directoryReader.leaves().size();
    }

    public int getSegmentMaxDoc(int segmentOrd) {
        if (directoryReader == null) return 0;
        var leaves = directoryReader.leaves();
        if (segmentOrd < 0 || segmentOrd >= leaves.size()) {
            throw new IllegalArgumentException("Invalid segment ordinal: " + segmentOrd);
        }
        return leaves.get(segmentOrd).reader().maxDoc();
    }

    // --- Result packaging ---

    private Iterator<VectorSchemaRoot> executeAllSegments(byte[] fragment) {
        try {
            LuceneIndexFilterContext ctx = getOrCreateContext(fragment);
            int totalMaxDoc = 0;
            for (int i = 0; i < ctx.segmentCount(); i++) {
                totalMaxDoc += ctx.segmentMaxDoc(i);
            }
            java.util.BitSet globalBitSet = new java.util.BitSet(totalMaxDoc);
            int docBase = 0;
            for (int seg = 0; seg < ctx.segmentCount(); seg++) {
                int segMaxDoc = ctx.segmentMaxDoc(seg);
                int collectorKey = filterProvider.createCollector(ctx, seg, 0, segMaxDoc);
                long[] bits = filterProvider.collectDocs(ctx, collectorKey, 0, segMaxDoc);
                filterProvider.releaseCollector(ctx, collectorKey);
                java.util.BitSet segBits = java.util.BitSet.valueOf(bits);
                for (int doc = segBits.nextSetBit(0); doc >= 0; doc = segBits.nextSetBit(doc + 1)) {
                    globalBitSet.set(docBase + doc);
                }
                docBase += segMaxDoc;
            }
            return createResultFromJavaBitSet(globalBitSet, totalMaxDoc);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed during Lucene query execution: " + e.getMessage(), e);
        }
    }

    private Iterator<VectorSchemaRoot> createEmptyResult() {
        BufferAllocator alloc = getAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(DOC_IDS_SCHEMA, alloc);
        BitVector docIds = (BitVector) root.getVector(DOC_IDS_COLUMN);
        docIds.allocateNew(0);
        docIds.setValueCount(0);
        root.setRowCount(0);
        return Collections.singletonList(root).iterator();
    }

    private Iterator<VectorSchemaRoot> createResultFromLongArray(long[] bits, int rangeSize) {
        return createResultFromJavaBitSet(java.util.BitSet.valueOf(bits), rangeSize);
    }

    private Iterator<VectorSchemaRoot> createResultFromJavaBitSet(java.util.BitSet bitSet, int totalDocs) {
        BufferAllocator alloc = getAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(DOC_IDS_SCHEMA, alloc);
        BitVector docIds = (BitVector) root.getVector(DOC_IDS_COLUMN);
        docIds.allocateNew(totalDocs);
        for (int i = 0; i < totalDocs; i++) {
            docIds.setSafe(i, bitSet.get(i) ? 1 : 0);
        }
        docIds.setValueCount(totalDocs);
        root.setRowCount(totalDocs);
        return Collections.singletonList(root).iterator();
    }
}

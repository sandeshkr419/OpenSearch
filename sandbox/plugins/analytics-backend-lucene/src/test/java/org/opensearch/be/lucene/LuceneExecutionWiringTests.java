/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import net.jqwik.api.Example;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.be.lucene.predicate.QueryBuilderSerializer;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;

import java.io.IOException;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for Lucene query execution wiring via LuceneFilterExecutor.
 */
class LuceneExecutionWiringTests {

    private static class InMemoryIndex implements AutoCloseable {
        final Directory directory;
        final DirectoryReader reader;

        InMemoryIndex(String fieldName, String... values) throws IOException {
            directory = new ByteBuffersDirectory();
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (String value : values) {
                    Document doc = new Document();
                    doc.add(new KeywordField(fieldName, value, Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            reader = DirectoryReader.open(directory);
        }

        @Override
        public void close() throws IOException {
            reader.close();
            directory.close();
        }
    }

    private static QueryShardContext createMockQSC(String fieldName) {
        QueryShardContext qsc = mock(QueryShardContext.class);
        KeywordFieldMapper.KeywordFieldType keywordFieldType =
            new KeywordFieldMapper.KeywordFieldType(fieldName);
        when(qsc.fieldMapper(fieldName)).thenReturn(keywordFieldType);
        return qsc;
    }

    @Example
    void executeWithoutInitializeReturnsEmptyResult() {
        LuceneFilterExecutor executor = new LuceneFilterExecutor();
        try {
            byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "GET"));
            Iterator<VectorSchemaRoot> results = executor.execute(serialized);
            assertThat(results.hasNext()).isTrue();
            VectorSchemaRoot root = results.next();
            assertThat(root.getRowCount()).isEqualTo(0);
            root.close();
        } finally {
            executor.close();
        }
    }

    @Example
    void executeAfterInitializeReturnsMatchingDocIds() throws IOException {
        try (InMemoryIndex idx = new InMemoryIndex("verb", "GET", "POST", "GET", "PUT")) {
            LuceneFilterExecutor executor = new LuceneFilterExecutor();
            executor.initialize(idx.reader, createMockQSC("verb"));
            try {
                byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "GET"));
                Iterator<VectorSchemaRoot> results = executor.execute(serialized);
                VectorSchemaRoot root = results.next();
                assertThat(root.getRowCount()).isEqualTo(4);
                BitVector docIds = (BitVector) root.getVector(LuceneFilterExecutor.DOC_IDS_COLUMN);
                assertThat(docIds.get(0)).isEqualTo(1);
                assertThat(docIds.get(1)).isEqualTo(0);
                assertThat(docIds.get(2)).isEqualTo(1);
                assertThat(docIds.get(3)).isEqualTo(0);
                root.close();
            } finally {
                executor.close();
            }
        }
    }

    @Example
    void executeWithZeroMatchReturnsAllZeroBitVector() throws IOException {
        try (InMemoryIndex idx = new InMemoryIndex("verb", "GET", "POST", "PUT")) {
            LuceneFilterExecutor executor = new LuceneFilterExecutor();
            executor.initialize(idx.reader, createMockQSC("verb"));
            try {
                byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "DELETE"));
                Iterator<VectorSchemaRoot> results = executor.execute(serialized);
                VectorSchemaRoot root = results.next();
                assertThat(root.getRowCount()).isEqualTo(3);
                BitVector docIds = (BitVector) root.getVector(LuceneFilterExecutor.DOC_IDS_COLUMN);
                for (int i = 0; i < 3; i++) {
                    assertThat(docIds.get(i)).isEqualTo(0);
                }
                root.close();
            } finally {
                executor.close();
            }
        }
    }

    @Example
    void executeWithAllMatchReturnsAllOnesBitVector() throws IOException {
        try (InMemoryIndex idx = new InMemoryIndex("verb", "GET", "GET", "GET")) {
            LuceneFilterExecutor executor = new LuceneFilterExecutor();
            executor.initialize(idx.reader, createMockQSC("verb"));
            try {
                byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "GET"));
                Iterator<VectorSchemaRoot> results = executor.execute(serialized);
                VectorSchemaRoot root = results.next();
                assertThat(root.getRowCount()).isEqualTo(3);
                BitVector docIds = (BitVector) root.getVector(LuceneFilterExecutor.DOC_IDS_COLUMN);
                for (int i = 0; i < 3; i++) {
                    assertThat(docIds.get(i)).isEqualTo(1);
                }
                root.close();
            } finally {
                executor.close();
            }
        }
    }
}

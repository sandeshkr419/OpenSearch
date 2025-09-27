/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class HyperLogLogFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new InternalSettingsPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "hyperloglog");
    }

    @Override
    protected Object getSampleValueForDocument() {
        try {
            // Create a simple HyperLogLog sketch with some test data
            byte[] sketch = HyperLogLogSerializer.createFromHashes(new long[]{1L, 2L, 3L, 4L, 5L}, 14);
            return Base64.getEncoder().encodeToString(sketch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "hyperloglog"));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", getSampleValueForDocument())));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertThat(fields[0], instanceOf(BinaryFieldMapper.CustomBinaryDocValuesField.class));
    }

    public void testPrecisionParameter() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "hyperloglog").field("precision", 12));
        DocumentMapper mapper = createDocumentMapper(mapping);
        
        HyperLogLogFieldMapper fieldMapper = (HyperLogLogFieldMapper) mapper.mappers().getMapper("field");
        HyperLogLogFieldMapper.HyperLogLogFieldType fieldType = 
            (HyperLogLogFieldMapper.HyperLogLogFieldType) fieldMapper.fieldType();
        assertEquals(12, fieldType.getPrecision());
    }

    public void testInvalidPrecision() throws Exception {
        // Test precision too low
        Exception e = expectThrows(MapperParsingException.class, () -> {
            createDocumentMapper(fieldMapping(b -> b.field("type", "hyperloglog").field("precision", 3)));
        });
        assertThat(e.getMessage(), containsString("precision must be between 4 and 18"));

        // Test precision too high
        e = expectThrows(MapperParsingException.class, () -> {
            createDocumentMapper(fieldMapping(b -> b.field("type", "hyperloglog").field("precision", 19)));
        });
        assertThat(e.getMessage(), containsString("precision must be between 4 and 18"));
    }

    public void testStoredField() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "hyperloglog").field("store", true));
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", getSampleValueForDocument())));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length); // One stored field, one doc values field
    }

    public void testNoDocValues() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "hyperloglog").field("doc_values", false));
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", getSampleValueForDocument())));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length); // No fields since neither stored nor doc_values
    }

    public void testBinaryValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        
        byte[] binaryData = HyperLogLogSerializer.createFromHashes(new long[]{100L, 200L, 300L}, 14);
        String base64Data = Base64.getEncoder().encodeToString(binaryData);
        
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", base64Data)));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
    }

    public void testInvalidBase64() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.field("field", "invalid-base64-data!")));
        });
        assertThat(e.getMessage(), containsString("Invalid base64 encoded HyperLogLog data"));
    }

    public void testTooShortData() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        
        // Create data that's too short (less than 4 bytes)
        String shortData = Base64.getEncoder().encodeToString(new byte[]{1, 2});
        
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(source(b -> b.field("field", shortData)));
        });
        assertThat(e.getMessage(), containsString("HyperLogLog data too short"));
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testMergeSketchesStatic() throws Exception {
        byte[] sketch1 = HyperLogLogSerializer.createFromHashes(new long[]{1L, 2L, 3L}, 14);
        byte[] sketch2 = HyperLogLogSerializer.createFromHashes(new long[]{4L, 5L, 6L}, 14);
        
        byte[] merged = HyperLogLogFieldMapper.HyperLogLogFieldType.mergeHyperLogLogSketches(sketch1, sketch2, 14);
        assertNotNull(merged);
        assertTrue(merged.length > 0);
    }

    public void testSerializerCardinality() throws Exception {
        long[] hashes = {1L, 2L, 3L, 4L, 5L, 100L, 200L, 300L, 400L, 500L};
        byte[] sketch = HyperLogLogSerializer.createFromHashes(hashes, 14);
        
        long cardinality = HyperLogLogSerializer.getCardinality(sketch);
        // HyperLogLog is approximate, so we check it's in a reasonable range
        assertTrue("Cardinality should be close to 10, got: " + cardinality, cardinality >= 8 && cardinality <= 12);
    }

    public void testSerializerPrecision() throws Exception {
        byte[] sketch = HyperLogLogSerializer.createFromHashes(new long[]{1L, 2L, 3L}, 12);
        int precision = HyperLogLogSerializer.getPrecision(sketch);
        assertEquals(12, precision);
    }

    public void testEmptySketch() throws Exception {
        byte[] emptySketch = HyperLogLogSerializer.createEmpty(14);
        long cardinality = HyperLogLogSerializer.getCardinality(emptySketch);
        assertEquals(0, cardinality);
    }

    public void testMergeWithNull() throws Exception {
        byte[] sketch = HyperLogLogSerializer.createFromHashes(new long[]{1L, 2L, 3L}, 14);
        
        // Test merge with null
        byte[] result1 = HyperLogLogSerializer.merge(sketch, null);
        assertArrayEquals(sketch, result1);
        
        byte[] result2 = HyperLogLogSerializer.merge(null, sketch);
        assertArrayEquals(sketch, result2);
        
        byte[] result3 = HyperLogLogSerializer.merge(null, null);
        assertNull(result3);
    }

    public void testMergeDifferentPrecisions() throws Exception {
        byte[] sketch1 = HyperLogLogSerializer.createFromHashes(new long[]{1L, 2L, 3L}, 12);
        byte[] sketch2 = HyperLogLogSerializer.createFromHashes(new long[]{4L, 5L, 6L}, 14);
        
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            HyperLogLogSerializer.merge(sketch1, sketch2);
        });
        assertThat(e.getMessage(), containsString("Cannot merge HyperLogLog sketches with different precisions"));
    }

    public void testFieldTypeTypeName() throws Exception {
        HyperLogLogFieldMapper.HyperLogLogFieldType fieldType = 
            new HyperLogLogFieldMapper.HyperLogLogFieldType("test");
        assertEquals("hyperloglog", fieldType.typeName());
    }

    public void testFieldTypeValueForDisplay() throws Exception {
        HyperLogLogFieldMapper.HyperLogLogFieldType fieldType = 
            new HyperLogLogFieldMapper.HyperLogLogFieldType("test");
        
        byte[] data = {1, 2, 3, 4};
        String base64 = Base64.getEncoder().encodeToString(data);
        
        BytesReference result = fieldType.valueForDisplay(base64);
        assertArrayEquals(data, result.toBytesRef().bytes);
    }

    public void testSearchNotSupported() throws Exception {
        HyperLogLogFieldMapper.HyperLogLogFieldType fieldType = 
            new HyperLogLogFieldMapper.HyperLogLogFieldType("test");
        
        expectThrows(UnsupportedOperationException.class, () -> {
            fieldType.termQuery("value", null);
        });
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }
}

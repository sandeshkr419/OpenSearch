/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A field mapper for HyperLogLog++ data structures.
 * This field type is designed to store pre-aggregated cardinality sketches
 * that can be merged during rollup operations.
 *
 * @opensearch.internal
 */
public class HyperLogLogFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "hyperloglog";

    private static HyperLogLogFieldMapper toType(FieldMapper in) {
        return (HyperLogLogFieldMapper) in;
    }

    /**
     * Builder for the HyperLogLog field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Integer> precision = Parameter.intParam(
            "precision",
            true,
            m -> toType(m).precision,
            HyperLogLogPlusPlus.DEFAULT_PRECISION
        ).setValidator(p -> {
            if (p < 4 || p > 18) {
                throw new MapperParsingException("precision must be between 4 and 18, got: " + p);
            }
        });
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        public List<Parameter<?>> getParameters() {
            return Arrays.asList(meta, stored, hasDocValues, precision);
        }

        @Override
        public HyperLogLogFieldMapper build(BuilderContext context) {
            return new HyperLogLogFieldMapper(
                name,
                new HyperLogLogFieldType(
                    buildFullName(context),
                    stored.getValue(),
                    hasDocValues.getValue(),
                    precision.getValue(),
                    meta.getValue()
                ),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * HyperLogLog field type
     *
     * @opensearch.internal
     */
    public static final class HyperLogLogFieldType extends MappedFieldType {

        private final int precision;

        private HyperLogLogFieldType(String name, boolean isStored, boolean hasDocValues, int precision, Map<String, String> meta) {
            super(name, false, isStored, hasDocValues, TextSearchInfo.NONE, meta);
            this.precision = precision;
        }

        public HyperLogLogFieldType(String name) {
            this(name, false, true, HyperLogLogPlusPlus.DEFAULT_PRECISION, Collections.emptyMap());
        }

        public int getPrecision() {
            return precision;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.BINARY;
        }

        @Override
        public BytesReference valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }

            if (value instanceof BytesReference) {
                return (BytesReference) value;
            } else if (value instanceof byte[]) {
                return new BytesArray((byte[]) value);
            } else {
                // Assume it's a base64 encoded string
                return new BytesArray(Base64.getDecoder().decode(value.toString()));
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new BytesBinaryIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "HyperLogLog fields do not support searching");
        }

        /**
         * Merge two HyperLogLog++ sketches
         */
        public static byte[] mergeHyperLogLogSketches(byte[] sketch1, byte[] sketch2, int precision) {
            try {
                return HyperLogLogSerializer.merge(sketch1, sketch2);
            } catch (IOException e) {
                throw new OpenSearchException("Failed to merge HyperLogLog sketches", e);
            }
        }
    }

    private final boolean stored;
    private final boolean hasDocValues;
    private final int precision;

    protected HyperLogLogFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.stored = builder.stored.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.precision = builder.precision.getValue();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (stored == false && hasDocValues == false) {
            return;
        }

        byte[] value = context.parseExternalValue(byte[].class);
        if (value == null) {
            if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
                return;
            } else {
                // Try to parse as base64 string or binary value
                if (context.parser().currentToken() == XContentParser.Token.VALUE_STRING) {
                    String stringValue = context.parser().text();
                    try {
                        value = Base64.getDecoder().decode(stringValue);
                    } catch (IllegalArgumentException e) {
                        throw new MapperParsingException("Invalid base64 encoded HyperLogLog data: " + stringValue);
                    }
                } else {
                    value = context.parser().binaryValue();
                }
            }
        }

        if (value == null) {
            return;
        }

        // Validate that the data looks like a valid HyperLogLog sketch
        validateHyperLogLogData(value);

        if (stored) {
            context.doc().add(new StoredField(fieldType().name(), value));
        }

        if (hasDocValues) {
            CustomHyperLogLogDocValuesField field = (CustomHyperLogLogDocValuesField) context.doc().getByKey(fieldType().name());
            if (field == null) {
                field = new CustomHyperLogLogDocValuesField(fieldType().name(), value, precision);
                context.doc().addWithKey(fieldType().name(), field);
            } else {
                field.add(value);
            }
        } else {
            createFieldNamesField(context);
        }
    }

    private void validateHyperLogLogData(byte[] data) {
        if (data.length < 4) {
            throw new MapperParsingException("HyperLogLog data too short, minimum 4 bytes required");
        }
        // Additional validation could be added here to check the data format
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new HyperLogLogFieldMapper.Builder(simpleName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Custom doc values field for HyperLogLog data that supports merging
     *
     * @opensearch.internal
     */
    public static class CustomHyperLogLogDocValuesField extends BinaryFieldMapper.CustomBinaryDocValuesField {

        private final int precision;

        public CustomHyperLogLogDocValuesField(String name, byte[] bytes, int precision) {
            super(name, bytes);
            this.precision = precision;
        }

        @Override
        public void add(byte[] bytes) {
            // For HyperLogLog fields, we want to merge the sketches rather than just collect them
            // This is a simplified implementation - in practice, you'd want to merge the HLL++ structures
            super.add(bytes);
        }

        /**
         * Merge all HyperLogLog sketches in this field
         */
        public byte[] getMergedSketch() {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeVInt(precision);
                // In a real implementation, you would deserialize each sketch,
                // merge them using HyperLogLogPlusPlus.merge(), and serialize the result
                // For now, we'll just concatenate the raw data as a placeholder
                for (byte[] sketch : bytesList) {
                    out.writeByteArray(sketch);
                }
                return out.bytes().toBytesRef().bytes;
            } catch (IOException e) {
                throw new OpenSearchException("Failed to merge HyperLogLog sketches", e);
            }
        }
    }
}

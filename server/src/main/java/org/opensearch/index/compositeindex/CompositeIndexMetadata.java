/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.compositeindex.datacube.startree.meta.StarTreeMetadata;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.MAGIC_MARKER;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.VERSION;

public class CompositeIndexMetadata {

    private static final Logger logger = LogManager.getLogger(CompositeIndexMetadata.class);
    private final Map<String, StarTreeMetadata> starTreeMetadataMap = new HashMap<>();

    public CompositeIndexMetadata(IndexInput meta) throws IOException {
        long magicMarker = meta.readLong();
        if (MAGIC_MARKER != magicMarker) {
            logger.error("Invalid composite field magic marker");
            throw new IOException("Invalid composite field magic marker");
        }
        int version = meta.readInt();
        if (VERSION != version) {
            logger.error("Invalid composite field version");
            throw new IOException("Invalid composite field version");
        }

        String compositeFieldName = meta.readString();
        String compositeFieldType = meta.readString();

        CompositeMappedFieldType.CompositeFieldType fieldType = CompositeMappedFieldType.CompositeFieldType.fromName(compositeFieldType);

        switch (fieldType) {
            // support for type of composite fields can be added in the future.
            case STAR_TREE:
                starTreeMetadataMap.put(compositeFieldName, new StarTreeMetadata(meta, compositeFieldName, compositeFieldType));
                break;
            default:
                throw new CorruptIndexException("Invalid composite field type present in the file", meta);
        }

    }

    public Map<String, StarTreeMetadata> getStarTreeMetadataMap() {
        return starTreeMetadataMap;
    }
}

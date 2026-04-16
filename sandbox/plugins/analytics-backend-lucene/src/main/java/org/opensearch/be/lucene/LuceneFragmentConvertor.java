/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.be.lucene.predicate.QueryBuilderSerializer;
import org.opensearch.index.query.MatchAllQueryBuilder;

/**
 * {@link FragmentConvertor} for the Lucene backend.
 *
 * <p>Converts RelNode fragments into serialized QueryBuilder bytes:
 * <ul>
 *   <li>If the fragment contains a {@link LogicalFilter}, extracts the
 *       filter condition and converts it via {@link LuceneFilterExecutor#convertFragment}
 *       using the {@link org.opensearch.be.lucene.predicate.PredicateHandlerRegistry}.</li>
 *   <li>If the fragment has no filter (e.g., a bare scan), falls back to
 *       {@link MatchAllQueryBuilder} (match all documents).</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class LuceneFragmentConvertor implements FragmentConvertor {

    private final LuceneFilterExecutor filterExecutor = new LuceneFilterExecutor();

    @Override
    public byte[] convertScanFragment(String tableName, RelNode fragment) {
        return convertOrMatchAll(fragment);
    }

    @Override
    public byte[] convertShuffleReadFragment(String tableName, RelNode fragment) {
        return convertOrMatchAll(fragment);
    }

    private byte[] convertOrMatchAll(RelNode fragment) {
        if (fragment instanceof LogicalFilter) {
            return filterExecutor.convertFragment(fragment);
        }
        // Walk children — the filter might be nested under a project
        for (RelNode input : fragment.getInputs()) {
            if (input instanceof LogicalFilter) {
                return filterExecutor.convertFragment(input);
            }
        }
        // TODO: throw an error instead of match all case.
        // Falling back to match all case temporarily
        return QueryBuilderSerializer.serialize(new MatchAllQueryBuilder());
    }
}

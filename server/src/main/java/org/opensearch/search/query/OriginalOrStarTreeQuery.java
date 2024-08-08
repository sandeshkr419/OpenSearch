/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.opensearch.search.query.startree.StarTreeQuery;

import java.io.IOException;
import java.util.List;

/**
 * Preserves star-tree queries which can be used along with original query
 * Decides which star-tree query to use (or not) based on cost factors
 */
public class OriginalOrStarTreeQuery extends Query implements Accountable {

    List<StarTreeQuery> starTreeQuery;
    Query originalQuery;
    private int starTreeQueryUsed;

    public OriginalOrStarTreeQuery(List<StarTreeQuery> starTreeQuery, Query originalQuery) {
        this.starTreeQuery = starTreeQuery;
        this.originalQuery = originalQuery;
        this.starTreeQueryUsed = -1;
    }

    @Override
    public String toString(String s) {
        return "";
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {

    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    public int getStarTreeQueryUsed() {
        return starTreeQueryUsed;
    }

    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
//        final Weight originalQueryWeight = this.originalQuery.createWeight(searcher, scoreMode, boost);

        
        this.starTreeQueryUsed = 0;
        final Weight starTreeQueryWeight = this.starTreeQuery.get(starTreeQueryUsed).createWeight(searcher, scoreMode, boost);

        // TODO: compare which query to use based on weights
        return starTreeQueryWeight;
    }
}

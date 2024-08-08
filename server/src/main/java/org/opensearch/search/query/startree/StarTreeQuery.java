/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.search.query.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;

/** Query class for querying star tree data structure */
@ExperimentalApi
public class StarTreeQuery extends Query implements Accountable {


    String fieldName;
    // List of valid star trees
    Map<String, List<Predicate<Long>>> compositePredicateMap;
    Set<String> groupByColumns;

    public StarTreeQuery(Map<String, List<Predicate<Long>>> compositePredicateMap, Set<String> groupByColumns) {
        this.compositePredicateMap = compositePredicateMap;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public String toString(String field) {
        return null;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
        return sameClassAs(obj);
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {


        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                SegmentReader reader = Lucene.segmentReader(context.reader());

                // We get the 'StarTreeReader' instance so that we can get StarTreeValues


                if(!(reader.getDocValuesReader() instanceof CompositeIndexReader)) return null;

                CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
                List<CompositeIndexFieldInfo> fiList = starTreeDocValuesReader.getCompositeIndexFields();
                StarTreeValues starTreeValues = null;
                if(fiList != null && !fiList.isEmpty()) {
                    starTreeValues = (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(fiList.get(0));
                } else {
                    return null;
                }

                DocIdSetIterator result = null;
                StarTreeFilter filter = new StarTreeFilter(starTreeValues, compositePredicateMap, groupByColumns);
                result = filter.getStarTreeResult();
                return new ConstantScoreScorer(this, score(), scoreMode, result);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }
}

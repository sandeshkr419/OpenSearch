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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.search.aggregations.startree.StarTreeAggregator;
//import org.opensearch.search.aggregations.startree.StarTreeAggregator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Filter operator for star tree data structure. */
public class StarTreeFilter {
    private static final Logger logger = LogManager.getLogger(StarTreeFilter.class);


    /** Helper class to wrap the result from traversing the star tree. */
    static class StarTreeResult {
        final DocIdSetBuilder _matchedDocIds;
        final Set<String> _remainingPredicateColumns;
        final int numOfMatchedDocs;
        final int maxMatchedDoc;

        StarTreeResult(DocIdSetBuilder matchedDocIds, Set<String> remainingPredicateColumns, int numOfMatchedDocs,
                       int maxMatchedDoc) {
            _matchedDocIds = matchedDocIds;
            _remainingPredicateColumns = remainingPredicateColumns;
            this.numOfMatchedDocs = numOfMatchedDocs;
            this.maxMatchedDoc = maxMatchedDoc;
        }
    }

//    private final StarTreeField _starTree;

    Map<String, List<Predicate<Long>>> _predicateEvaluators;
    private final Set<String> _groupByColumns;

    DocIdSetBuilder docsWithField;

    DocIdSetBuilder.BulkAdder adder;
    Map<String, SortedNumericDocValues> dimValueMap;
    public StarTreeFilter(
        Map<String, List<Predicate<Long>>> predicateEvaluators,
        Set<String> groupByColumns
    ) throws IOException {
        // This filter operator does not support AND/OR/NOT operations.
//        _starTree = starTreeAggrStructure._starTree;
//        dimValueMap = starTreeAggrStructure.dimensionValues;
        _predicateEvaluators = predicateEvaluators != null ? predicateEvaluators : Collections.emptyMap();
        _groupByColumns = groupByColumns != null ? groupByColumns : Collections.emptySet();

        // TODO : this should be the maximum number of doc values
        docsWithField = new DocIdSetBuilder(Integer.MAX_VALUE);
    }

    /**
     * <ul>
     *   <li>First go over the star tree and try to match as many dimensions as possible
     *   <li>For the remaining columns, use doc values indexes to match them
     * </ul>
     */
    public DocIdSetIterator getStarTreeResult() throws IOException {
        StarTreeResult starTreeResult = traverseStarTree();
        //logger.info("Matched docs in star tree : {}" , starTreeResult.numOfMatchedDocs);
        List<DocIdSetIterator> andIterators = new ArrayList<>();
        andIterators.add(starTreeResult._matchedDocIds.build().iterator());
        DocIdSetIterator docIdSetIterator = andIterators.get(0);
        // No matches, return
        if(starTreeResult.maxMatchedDoc == -1) {
            return docIdSetIterator;
        }
        int docCount = 0;
        for (String remainingPredicateColumn : starTreeResult._remainingPredicateColumns) {
            // TODO : set to max value of doc values
            logger.info("remainingPredicateColumn : {}, maxMatchedDoc : {} ", remainingPredicateColumn, starTreeResult.maxMatchedDoc);
            DocIdSetBuilder builder = new DocIdSetBuilder(starTreeResult.maxMatchedDoc + 1);
            List<Predicate<Long>> compositePredicateEvaluators = _predicateEvaluators.get(remainingPredicateColumn);
            SortedNumericDocValues ndv = this.dimValueMap.get(remainingPredicateColumn);
            List<Integer> docIds = new ArrayList<>();
            while (docIdSetIterator.nextDoc() != NO_MORE_DOCS) {
                docCount++;
                int docID = docIdSetIterator.docID();
                if(ndv.advanceExact(docID)) {
                    final int valuesCount = ndv.docValueCount();
                    long value = ndv.nextValue();
                    for (Predicate<Long> compositePredicateEvaluator : compositePredicateEvaluators) {
                        // TODO : this might be expensive as its done against all doc values docs
                        if (compositePredicateEvaluator.test(value)) {
                            docIds.add(docID);
                            for (int i = 0; i < valuesCount - 1; i++) {
                                while(docIdSetIterator.nextDoc() != NO_MORE_DOCS) {
                                    docIds.add(docIdSetIterator.docID());
                                }
                            }
                            break;
                        }
                    }
                }
            }
            DocIdSetBuilder.BulkAdder adder = builder.grow(docIds.size());
            for(int docID : docIds) {
                adder.add(docID);
            }
            docIdSetIterator = builder.build().iterator();
        }
        return docIdSetIterator;
    }

    /**
     * Helper method to traverse the star tree, get matching documents and keep track of all the
     * predicate dimensions that are not matched.
     */
    private StarTreeResult traverseStarTree() throws IOException {
        return new StarTreeResult(
            docsWithField,
            Collections.emptySet(),
            0,
            0
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.predicate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/**
 * Handles boolean combinations of Lucene predicates:
 * AND → BoolQueryBuilder with must clauses,
 * OR → BoolQueryBuilder with should clauses,
 * NOT → BoolQueryBuilder with mustNot clause.
 *
 * <p>Recursively converts child RexNodes via {@link RexToQueryBuilderConverter}.
 */
public class BoolPredicateHandler implements PredicateHandler {

    private final SqlKind kind;

    private BoolPredicateHandler(SqlKind kind) {
        this.kind = kind;
    }

    public static BoolPredicateHandler and() {
        return new BoolPredicateHandler(SqlKind.AND);
    }

    public static BoolPredicateHandler or() {
        return new BoolPredicateHandler(SqlKind.OR);
    }

    public static BoolPredicateHandler not() {
        return new BoolPredicateHandler(SqlKind.NOT);
    }

    @Override
    public SqlKind sqlKind() {
        return kind;
    }

    @Override
    public SqlOperator sqlOperator() {
        return switch (kind) {
            case AND -> SqlStdOperatorTable.AND;
            case OR -> SqlStdOperatorTable.OR;
            case NOT -> SqlStdOperatorTable.NOT;
            default -> throw new IllegalStateException("Unexpected kind: " + kind);
        };
    }

    @Override
    public boolean canHandle(RexCall call, RelDataType inputRowType, MapperService mapperService) {
        if (call.getKind() != kind) {
            return false;
        }
        // All children must be Lucene-handleable
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexCall childCall) {
                if (PredicateHandlerRegistry.canHandle(childCall, inputRowType, mapperService) == false) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public QueryBuilder convert(RexCall call, RelDataType inputRowType, MapperService mapperService) {
        RexToQueryBuilderConverter converter = new RexToQueryBuilderConverter(inputRowType, mapperService);
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        for (RexNode operand : call.getOperands()) {
            QueryBuilder child = converter.convert(operand);
            switch (kind) {
                case AND -> boolQuery.must(child);
                case OR -> boolQuery.should(child);
                case NOT -> boolQuery.mustNot(child);
                default -> throw new IllegalStateException("Unexpected kind: " + kind);
            }
        }

        return boolQuery;
    }

    @Override
    public List<NamedWriteableRegistry.Entry> namedWriteableEntries() {
        // Only AND handler registers BoolQueryBuilder to avoid duplicate entries
        if (kind == SqlKind.AND) {
            return List.of(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, BoolQueryBuilder.NAME, BoolQueryBuilder::new)
            );
        }
        return List.of();
    }
}

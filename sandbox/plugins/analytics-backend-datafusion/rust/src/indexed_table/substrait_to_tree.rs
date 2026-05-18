/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Substrait → boolean tree conversion.
//!
//! After Substrait is decoded into a DataFusion `LogicalPlan`, the filter
//! expression is a tree of `Expr` nodes. This module walks that tree and
//! classifies each node:
//!
//! - `AND` / `OR` / `NOT` → `BoolNode::And` / `Or` / `Not`
//! - `ScalarFunction` named `COLLECTOR_FUNCTION_NAME` with one `Binary`
//!   literal argument → `BoolNode::Collector { annotation_id }`. The ID
//!   are the serialized backend query payload; they're handed to a Java
//!   factory at query-resolve time to create a provider.
//! - **Anything else** → lowered to [`Arc<dyn PhysicalExpr>`] via
//!   DataFusion's `create_physical_expr`, wrapped in
//!   [`BoolNode::Predicate`]. Comparisons, `IS NULL`, `IN`, `BETWEEN`,
//!   arithmetic, casts, UDFs — any boolean-valued DataFusion expression
//!   is accepted.
//!
//! **The substrait plan is the wire format.** Java never serializes an
//! `IndexFilterTree`; it rewrites `column = 'value'` on indexed columns to
//! `delegated_predicate(annotationId)` UDF calls during the Calcite marking phase,
//! and that survives the substrait round-trip. Rust just reads it back out
//! of the decoded `LogicalPlan`.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::{Between, InList, Like};
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::create_physical_expr;
#[cfg(test)]
use datafusion::physical_expr::PhysicalExpr;

use super::bool_tree::BoolNode;

/// The UDF name Calcite emits for indexed-column filter markers.
pub const COLLECTOR_FUNCTION_NAME: &str = "delegated_predicate";

/// Classification of a query's filter expression — drives the evaluator choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterClass {
    /// Zero `index_filter` calls. Path A — regular DataFusion `ListingTable`,
    /// no `IndexedTableProvider` involvement.
    None,
    /// Exactly one `index_filter`, AND'd with parquet-native predicates.
    /// Path B — `SingleCollectorEvaluator`, DataFusion handles residual via
    /// predicate pushdown.
    SingleCollector,
    /// Multiple `index_filter` calls, or OR/NOT mixing them with predicates.
    /// Path C — `BitmapTreeEvaluator` two-phase evaluation.
    Tree,
}

/// Result of `expr_to_bool_tree` — just the tree. No sidecar list needed
/// now that `Predicate` leaves carry `Arc<dyn PhysicalExpr>` directly.
#[derive(Debug)]
pub struct ExtractionResult {
    pub tree: BoolNode,
}

/// Extract the filter expression from a DataFusion logical plan.
///
/// Walks down through Projection/SubqueryAlias/etc. nodes to find the first
/// `Filter` node. Returns `None` if there's no filter.
pub fn extract_filter_expr(plan: &LogicalPlan) -> Option<Expr> {
    match plan {
        LogicalPlan::Filter(filter) => Some(filter.predicate.clone()),
        _ => {
            for child in plan.inputs() {
                if let Some(expr) = extract_filter_expr(child) {
                    return Some(expr);
                }
            }
            None
        }
    }
}

/// Convert a DataFusion filter `Expr` to a `BoolNode` tree.
///
/// `schema` is used to lower non-combinator subexpressions to
/// `Arc<dyn PhysicalExpr>` via `create_physical_expr`. The expression at
/// those leaves must be boolean-valued; anything else is rejected.
pub fn expr_to_bool_tree(expr: &Expr, schema: &SchemaRef) -> Result<ExtractionResult, String> {
    let df_schema =
        DFSchema::try_from(schema.as_ref().clone()).map_err(|e| format!("DFSchema: {}", e))?;
    let props = ExecutionProps::new();
    // Strip table qualifiers up front. Substrait's NamedScan-derived field
    // references qualify columns with the table name (e.g.
    // "test_table.elb_status_code"), but the parquet schema (and thus the
    // DFSchema we look types up in) carries bare names. Stripping here is
    // necessary so the literal-promotion pre-pass below can resolve column
    // types via the schema.
    let unqualified = strip_column_qualifiers(expr);
    // Promote substrait `Utf8` / `LargeUtf8` literals to `Utf8View` when
    // they participate in a comparison/LIKE/IN/BETWEEN against a
    // `Utf8View` column. See `promote_string_literals_for_view_columns`
    // for the full rationale; in short: substrait has no Utf8View
    // literal type, but the parquet leaf in this session emits Utf8View
    // for string columns when `schema_force_view_types` is on, and
    // arrow's comparison/LIKE kernels reject mixed string variants.
    let promoted = promote_string_literals_for_view_columns(unqualified, &df_schema)?;
    let tree = convert_expr(&promoted, schema, &df_schema, &props)?;
    Ok(ExtractionResult { tree })
}

fn convert_expr(
    expr: &Expr,
    schema: &Schema,
    df_schema: &DFSchema,
    props: &ExecutionProps,
) -> Result<BoolNode, String> {
    match expr {
        Expr::BinaryExpr(bin) if bin.op == Operator::And => {
            let left = convert_expr(&bin.left, schema, df_schema, props)?;
            let right = convert_expr(&bin.right, schema, df_schema, props)?;
            Ok(BoolNode::And(vec![left, right]))
        }
        Expr::BinaryExpr(bin) if bin.op == Operator::Or => {
            let left = convert_expr(&bin.left, schema, df_schema, props)?;
            let right = convert_expr(&bin.right, schema, df_schema, props)?;
            Ok(BoolNode::Or(vec![left, right]))
        }
        Expr::Not(inner) => {
            let child = convert_expr(inner, schema, df_schema, props)?;
            Ok(BoolNode::Not(Box::new(child)))
        }
        Expr::ScalarFunction(func) if func.name() == COLLECTOR_FUNCTION_NAME => {
            convert_collector_function(&func.args)
        }
        // Anything else — comparison, IS NULL, IN, BETWEEN, arithmetic,
        // CAST, UDF — gets lowered to a DataFusion PhysicalExpr. We
        // require boolean return type so the tree evaluator can
        // interpret the result as a per-row mask.
        other => {
            // Strip table qualifiers from Column references. DataFusion's
            // substrait consumer qualifies field references with the
            // NamedScan table name (e.g. "test_table.elb_status_code"),
            // but the parquet schema has bare names. Without stripping,
            // `create_physical_expr` fails with "No field named ...".
            let unqualified = strip_column_qualifiers(other);
            let phys = create_physical_expr(&unqualified, df_schema, props)
                .map_err(|e| format!("create_physical_expr for {:?}: {}", unqualified, e))?;
            let return_type = phys
                .data_type(schema)
                .map_err(|e| format!("data_type: {}", e))?;
            if return_type != DataType::Boolean {
                return Err(format!(
                    "indexed-query expression must be boolean-valued, got {:?}: {:?}",
                    return_type, other
                ));
            }
            Ok(BoolNode::Predicate(phys))
        }
    }
}

/// `delegated_predicate(annotationId)` — a single `Int32` literal arg.
fn convert_collector_function(args: &[Expr]) -> Result<BoolNode, String> {
    if args.len() != 1 {
        return Err(format!(
            "{} expects 1 arg (annotationId), got {}",
            COLLECTOR_FUNCTION_NAME,
            args.len()
        ));
    }
    let annotation_id = extract_int32_literal(&args[0])?;
    Ok(BoolNode::Collector { annotation_id })
}

/// Strip table qualifiers from `Column` references in an `Expr` tree.
/// DataFusion's substrait consumer qualifies field references with the
/// NamedScan table name, but the parquet schema has bare column names.
fn strip_column_qualifiers(expr: &Expr) -> Expr {
    expr.clone()
        .transform(|e| {
            if let Expr::Column(col) = &e {
                if col.relation.is_some() {
                    return Ok(datafusion::common::tree_node::Transformed::yes(
                        Expr::Column(datafusion::common::Column::new_unqualified(&col.name)),
                    ));
                }
            }
            Ok(datafusion::common::tree_node::Transformed::no(e))
        })
        .unwrap()
        .data
}

/// Promote substrait `Utf8` / `LargeUtf8` string literals to `Utf8View`
/// when they participate in a comparison / LIKE / IN / BETWEEN against an
/// expression whose type is `Utf8View`.
///
/// **Why this exists.** Substrait literals always decode to `Utf8`
/// (substrait has no `Utf8View` variant). The parquet leaf in this session
/// emits `Utf8View` for string columns when `schema_force_view_types` is
/// on. Without promotion, the resulting `BinaryExpr` / `LikeExpr` would
/// have mismatched string operands and arrow rejects mixed-variant
/// comparisons at runtime with errors like:
///
/// - `Invalid comparison operation: Utf8View == Utf8` (equality on keyword
///   fields),
/// - `Utf8View AND Utf8 of like physical should be same` (LIKE on text
///   fields).
///
/// **Why not run DataFusion's `TypeCoercionRewriter` here.** That rewriter
/// would insert a `Cast(literal AS Utf8View)` node, and it would survive
/// into the physical expression because this path bypasses the optimizer
/// (predicate const-folding never runs). Retagging the literal directly
/// produces the same end state — `Utf8View` literal compared to `Utf8View`
/// column — with no per-batch `Cast` evaluation in the bool tree's
/// `BoolNode::Predicate` leaves or the parquet pushdown predicate handed
/// to `ParquetSource::with_predicate`.
///
/// **Scope.** Comparison `BinaryExpr`s (the `=`, `<>`, `<`, `<=`, `>`,
/// `>=` family), `Like`, `Between`, and `InList`. AND/OR/NOT and other
/// non-string operators pass through unchanged.
fn promote_string_literals_for_view_columns(
    expr: Expr,
    df_schema: &DFSchema,
) -> Result<Expr, String> {
    let result = expr
        .transform(|e| {
            let new_e = match e {
                Expr::BinaryExpr(BinaryExpr { left, op, right })
                    if is_string_comparison_operator(&op) =>
                {
                    let left_ty = left.get_type(df_schema).ok();
                    let right_ty = right.get_type(df_schema).ok();
                    let new_left = retag_if_peer_is_view(*left, &right_ty);
                    let new_right = retag_if_peer_is_view(*right, &left_ty);
                    Expr::BinaryExpr(BinaryExpr::new(Box::new(new_left), op, Box::new(new_right)))
                }
                Expr::Like(like) => {
                    let expr_ty = like.expr.get_type(df_schema).ok();
                    let pat_ty = like.pattern.get_type(df_schema).ok();
                    let new_expr = retag_if_peer_is_view(*like.expr, &pat_ty);
                    let new_pattern = retag_if_peer_is_view(*like.pattern, &expr_ty);
                    Expr::Like(Like {
                        negated: like.negated,
                        expr: Box::new(new_expr),
                        pattern: Box::new(new_pattern),
                        escape_char: like.escape_char,
                        case_insensitive: like.case_insensitive,
                    })
                }
                Expr::Between(between) => {
                    let expr_ty = between.expr.get_type(df_schema).ok();
                    let new_low = retag_if_peer_is_view(*between.low, &expr_ty);
                    let new_high = retag_if_peer_is_view(*between.high, &expr_ty);
                    Expr::Between(Between {
                        expr: between.expr,
                        negated: between.negated,
                        low: Box::new(new_low),
                        high: Box::new(new_high),
                    })
                }
                Expr::InList(in_list) => {
                    let expr_ty = in_list.expr.get_type(df_schema).ok();
                    let new_list: Vec<Expr> = in_list
                        .list
                        .into_iter()
                        .map(|e| retag_if_peer_is_view(e, &expr_ty))
                        .collect();
                    Expr::InList(InList {
                        expr: in_list.expr,
                        list: new_list,
                        negated: in_list.negated,
                    })
                }
                other => return Ok(Transformed::no(other)),
            };
            Ok(Transformed::yes(new_e))
        })
        .map_err(|e| format!("promote_string_literals: {}", e))?;
    Ok(result.data)
}

/// If `peer_type` is `Utf8View` and `expr` is a `Utf8` / `LargeUtf8`
/// literal (including `NULL` of those types), retag the literal to
/// `Utf8View`. Otherwise return `expr` unchanged.
fn retag_if_peer_is_view(expr: Expr, peer_type: &Option<DataType>) -> Expr {
    if !matches!(peer_type, Some(DataType::Utf8View)) {
        return expr;
    }
    match expr {
        Expr::Literal(ScalarValue::Utf8(s), m) => Expr::Literal(ScalarValue::Utf8View(s), m),
        Expr::Literal(ScalarValue::LargeUtf8(s), m) => Expr::Literal(ScalarValue::Utf8View(s), m),
        other => other,
    }
}

/// True for the comparison / inequality operators where mixed string
/// variants between operands cause runtime kernel errors. AND / OR /
/// arithmetic / string-concat / bitwise are excluded.
fn is_string_comparison_operator(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::LikeMatch
            | Operator::NotLikeMatch
            | Operator::ILikeMatch
            | Operator::NotILikeMatch
            | Operator::RegexMatch
            | Operator::RegexNotMatch
            | Operator::RegexIMatch
            | Operator::RegexNotIMatch
    )
}

fn extract_int32_literal(expr: &Expr) -> Result<i32, String> {
    match expr {
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(*v),
        _ => Err(format!(
            "{} arg must be an Int32 literal, got {:?}",
            COLLECTOR_FUNCTION_NAME, expr
        )),
    }
}

/// Classify a filter tree to decide which execution path to take.
///
/// - 0 collector leaves → `FilterClass::None`
/// - bare collector → `FilterClass::SingleCollector`
/// - any AND-only tree (no OR/NOT above collectors) with ≥1 collector
///   → `FilterClass::SingleCollector`. Nested ANDs with mixed
///   collectors + predicates are accepted; `single_collector_bytes`
///   merges the collectors and `extract_single_collector_residual`
///   strips them to produce the predicate residual.
/// - anything else (OR / NOT above a collector) → `FilterClass::Tree`
pub fn classify_filter(tree: &BoolNode) -> FilterClass {
    if tree.collector_leaf_count() == 0 {
        return FilterClass::None;
    }
    if matches!(tree, BoolNode::Collector { .. }) {
        return FilterClass::SingleCollector;
    }
    if is_and_only_collector_tree(tree) {
        FilterClass::SingleCollector
    } else {
        FilterClass::Tree
    }
}

/// Returns true when every collector in `tree` is reachable only
/// through AND nodes (no OR or NOT on the path from root to any
/// collector leaf). Predicates, ANDs, and collector leaves are fine;
/// OR or NOT containing a collector disqualifies.
fn is_and_only_collector_tree(tree: &BoolNode) -> bool {
    match tree {
        BoolNode::And(children) => children.iter().all(is_and_only_collector_tree),
        BoolNode::Collector { .. } | BoolNode::Predicate(_) => true,
        // OR or NOT containing any collector → Tree path.
        BoolNode::Or(_) | BoolNode::Not(_) => tree.collector_leaf_count() == 0,
    }
}

/// Create the `delegated_predicate(annotationId) → Boolean` UDF.
///
/// This UDF exists solely as a marker for `classify_filter` / `expr_to_bool_tree`.
/// Its body is deliberately wired to return a hard `DataFusionError` if it
/// ever executes, because a production execution of the body would silently
/// produce all-true results and mask a routing bug in the dispatcher.
/// Register in a `SessionContext` before decoding substrait plans that
/// contain the UDF.
pub fn create_index_filter_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(IndexFilterUdf::new())
}

#[derive(Debug)]
struct IndexFilterUdf {
    signature: Signature,
}

impl IndexFilterUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Int32])],
                Volatility::Immutable,
            ),
        }
    }
}

impl std::hash::Hash for IndexFilterUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl PartialEq for IndexFilterUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

impl Eq for IndexFilterUdf {}

impl ScalarUDFImpl for IndexFilterUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        COLLECTOR_FUNCTION_NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        // This body must never execute in production. `classify_filter`
        // recognizes the UDF by name and routes to the indexed evaluator;
        // when it works correctly, DataFusion never evaluates the UDF as a
        // function. If we reach here, classification missed the marker and
        // would otherwise silently return all-true, masking the bug and
        // producing wrong results. Fail loudly instead.
        Err(datafusion::common::DataFusionError::Internal(format!(
            "{} UDF body invoked — classify_filter did not recognize the marker; \
                 treat as a serious correctness bug",
            COLLECTOR_FUNCTION_NAME
        )))
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_expr::expressions::{Column as PhysColumn, Literal};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    // ── expr_to_bool_tree ────────────────────────────────────────────

    #[test]
    fn simple_predicate() {
        let expr = col("price").gt(lit(100i32));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn literal_op_column_works() {
        // 100 < price — valid boolean expression, lowered as-is.
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(lit(100i32)),
            Operator::Lt,
            Box::new(col("price")),
        ));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn and_of_predicates() {
        let expr = col("price").gt(lit(100i32)).and(col("qty").lt(lit(50i32)));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::And(_)));
    }

    #[test]
    fn not_predicate() {
        let expr = Expr::Not(Box::new(col("active").eq(lit(true))));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Not(_)));
    }

    #[test]
    fn in_list_expression_is_accepted() {
        let expr = col("price").in_list(vec![lit(5i32), lit(10i32), lit(15i32)], false);
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn is_null_expression_is_accepted() {
        let expr = Expr::IsNull(Box::new(col("price")));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn between_expression_is_accepted() {
        // price BETWEEN 10 AND 50
        let expr = col("price").between(lit(10i32), lit(50i32));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        // BETWEEN may desugar into And internally or stay as-is; either
        // shape is accepted so long as the result is boolean-valued.
        match r.tree {
            BoolNode::Predicate(_) | BoolNode::And(_) => {}
            other => panic!("expected Predicate or And, got {:?}", other),
        }
    }

    #[test]
    fn arithmetic_comparison_is_accepted() {
        // (price + 10) > 100 — our old converter would reject this.
        let expr = (col("price") + lit(10i32)).gt(lit(100i32));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn non_boolean_expression_is_rejected() {
        // `price + 10` on its own is Int32, not Boolean → must error.
        let expr = col("price") + lit(10i32);
        let r = expr_to_bool_tree(&expr, &test_schema());
        assert!(r.is_err());
        let e = r.unwrap_err();
        assert!(e.contains("boolean"), "got: {}", e);
    }

    #[test]
    fn collector_function() {
        let udf = Arc::new(create_index_filter_udf());
        let expr = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            udf,
            vec![lit(ScalarValue::Int32(Some(42)))],
        ));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        match r.tree {
            BoolNode::Collector { annotation_id } => {
                assert_eq!(annotation_id, 42);
            }
            _ => panic!("expected Collector"),
        }
    }

    #[test]
    fn mixed_tree() {
        // AND(collector(annotationId), OR(price > 100, qty < 50))
        let udf = Arc::new(create_index_filter_udf());
        let collector_expr =
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                udf,
                vec![lit(ScalarValue::Int32(Some(0)))],
            ));
        let or_branch = col("price").gt(lit(100i32)).or(col("qty").lt(lit(50i32)));
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(collector_expr),
            Operator::And,
            Box::new(or_branch),
        ));
        let r = expr_to_bool_tree(&expr, &test_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::And(_)));
    }

    // ── promote_string_literals_for_view_columns ─────────────────────
    //
    // Substrait literals always decode to `Utf8`; this session's parquet
    // leaf emits `Utf8View` for string columns. The promote pass is what
    // bridges that gap before `create_physical_expr`. These tests exercise
    // the pass directly and end-to-end through `expr_to_bool_tree`.

    fn view_string_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8View, true),
            Field::new("city", DataType::Utf8View, true),
            Field::new("legacy_name", DataType::Utf8, true),
            Field::new("price", DataType::Int32, false),
        ]))
    }

    fn df_schema_for(s: &SchemaRef) -> DFSchema {
        DFSchema::try_from(s.as_ref().clone()).unwrap()
    }

    fn assert_is_utf8view_literal(expr: &Expr, expected: &str) {
        match expr {
            Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => assert_eq!(s, expected),
            other => panic!("expected Utf8View literal {:?}, got {:?}", expected, other),
        }
    }

    fn assert_is_utf8_literal(expr: &Expr, expected: &str) {
        match expr {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => assert_eq!(s, expected),
            other => panic!("expected Utf8 literal {:?}, got {:?}", expected, other),
        }
    }

    #[test]
    fn promote_eq_view_column_against_utf8_literal() {
        // name[Utf8View] = 'B8'(Utf8) → both Utf8View
        let expr = col("name").eq(lit("B8"));
        let schema = view_string_schema();
        let dfs = df_schema_for(&schema);
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => {
                assert_is_utf8view_literal(right, "B8");
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn promote_eq_handles_swapped_operands() {
        // 'B8'(Utf8) = name[Utf8View] → literal still promoted (mirror direction).
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit("B8")),
            Operator::Eq,
            Box::new(col("name")),
        ));
        let schema = view_string_schema();
        let dfs = df_schema_for(&schema);
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::BinaryExpr(BinaryExpr { left, .. }) => {
                assert_is_utf8view_literal(left, "B8");
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn promote_large_utf8_literal_against_view_column() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("name")),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::LargeUtf8(Some("B8".to_string())),
                None,
            )),
        ));
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => {
                assert_is_utf8view_literal(right, "B8");
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn promote_preserves_null_literal_type_widening() {
        // name[Utf8View] = NULL(Utf8) → NULL(Utf8View). Null literals must
        // also widen so the kernel sees matching variants.
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("name")),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::Utf8(None), None)),
        ));
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => match right.as_ref() {
                Expr::Literal(ScalarValue::Utf8View(None), _) => {}
                other => panic!("expected Utf8View(None), got {:?}", other),
            },
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn promote_skips_non_view_string_column() {
        // legacy_name[Utf8] = 'B8'(Utf8) → unchanged; both already Utf8.
        let expr = col("legacy_name").eq(lit("B8"));
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => {
                assert_is_utf8_literal(right, "B8");
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn promote_skips_non_string_literal() {
        // price[Int32] = 100(Int32) → unchanged; literal is not a string.
        let expr = col("price").eq(lit(100i32));
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr.clone(), &dfs).unwrap();
        match &promoted {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => match right.as_ref() {
                Expr::Literal(ScalarValue::Int32(Some(100)), _) => {}
                other => panic!("expected Int32 literal, got {:?}", other),
            },
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn promote_covers_all_comparison_operators() {
        // Each of =, !=, <, <=, >, >= should retag the literal sibling.
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            let expr = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("name")),
                op,
                Box::new(lit("B8")),
            ));
            let dfs = df_schema_for(&view_string_schema());
            let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
            match &promoted {
                Expr::BinaryExpr(BinaryExpr { right, .. }) => {
                    assert_is_utf8view_literal(right, "B8");
                }
                other => panic!("op {:?}: expected BinaryExpr, got {:?}", op, other),
            }
        }
    }

    #[test]
    fn promote_skips_non_comparison_binary_operator() {
        // String concat is not a comparison; type coercion is the kernel's
        // problem. Leave the literal alone.
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("name")),
            Operator::StringConcat,
            Box::new(lit("_suffix")),
        ));
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::BinaryExpr(BinaryExpr { right, .. }) => {
                assert_is_utf8_literal(right, "_suffix");
            }
            other => panic!("expected BinaryExpr, got {:?}", other),
        }
    }

    #[test]
    fn promote_like_pattern_against_view_column() {
        // name[Utf8View] LIKE 'goog%'(Utf8) → pattern promoted to Utf8View.
        let expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(lit("goog%")),
            escape_char: None,
            case_insensitive: false,
        });
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::Like(Like { pattern, .. }) => {
                assert_is_utf8view_literal(pattern, "goog%");
            }
            other => panic!("expected Like, got {:?}", other),
        }
    }

    #[test]
    fn promote_between_bounds_against_view_column() {
        // name[Utf8View] BETWEEN 'a'(Utf8) AND 'z'(Utf8) → both promoted.
        let expr = Expr::Between(Between {
            expr: Box::new(col("name")),
            negated: false,
            low: Box::new(lit("a")),
            high: Box::new(lit("z")),
        });
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::Between(Between { low, high, .. }) => {
                assert_is_utf8view_literal(low, "a");
                assert_is_utf8view_literal(high, "z");
            }
            other => panic!("expected Between, got {:?}", other),
        }
    }

    #[test]
    fn promote_in_list_against_view_column() {
        // name[Utf8View] IN ('B8', 'US', 'IN')(Utf8) → all promoted.
        let expr = Expr::InList(InList {
            expr: Box::new(col("name")),
            list: vec![lit("B8"), lit("US"), lit("IN")],
            negated: false,
        });
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();
        match &promoted {
            Expr::InList(InList { list, .. }) => {
                assert_eq!(list.len(), 3);
                assert_is_utf8view_literal(&list[0], "B8");
                assert_is_utf8view_literal(&list[1], "US");
                assert_is_utf8view_literal(&list[2], "IN");
            }
            other => panic!("expected InList, got {:?}", other),
        }
    }

    #[test]
    fn promote_recurses_through_and_or_not() {
        // (name = 'B8') AND ((city = 'NYC') OR NOT(name = 'X'))
        // — every comparison's literal is promoted independently.
        let expr = col("name").eq(lit("B8")).and(
            col("city")
                .eq(lit("NYC"))
                .or(Expr::Not(Box::new(col("name").eq(lit("X"))))),
        );
        let dfs = df_schema_for(&view_string_schema());
        let promoted = promote_string_literals_for_view_columns(expr, &dfs).unwrap();

        // Walk and collect every literal we see; assert all are Utf8View.
        let mut literal_kinds: Vec<&'static str> = Vec::new();
        promoted
            .clone()
            .apply(|e| {
                if let Expr::Literal(sv, _) = e {
                    literal_kinds.push(match sv {
                        ScalarValue::Utf8View(_) => "Utf8View",
                        ScalarValue::Utf8(_) => "Utf8",
                        ScalarValue::LargeUtf8(_) => "LargeUtf8",
                        _ => "other",
                    });
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert!(
            literal_kinds.iter().all(|k| *k == "Utf8View"),
            "expected every string literal to be Utf8View, got {:?}",
            literal_kinds
        );
        assert_eq!(literal_kinds.len(), 3);
    }

    // ── End-to-end through expr_to_bool_tree ─────────────────────────
    //
    // Pre-fix: each of these reproduced the runtime errors quoted in the
    // PR description. Post-fix, `create_physical_expr` succeeds because
    // both operands are Utf8View by the time it runs.

    #[test]
    fn e2e_eq_view_column_with_utf8_literal_does_not_fail() {
        let expr = col("name").eq(lit("B8"));
        let r = expr_to_bool_tree(&expr, &view_string_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn e2e_like_view_column_with_utf8_pattern_does_not_fail() {
        let expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(lit("goog%")),
            escape_char: None,
            case_insensitive: false,
        });
        let r = expr_to_bool_tree(&expr, &view_string_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn e2e_in_list_view_column_with_utf8_literals_does_not_fail() {
        let expr = Expr::InList(InList {
            expr: Box::new(col("name")),
            list: vec![lit("B8"), lit("US")],
            negated: false,
        });
        let r = expr_to_bool_tree(&expr, &view_string_schema()).unwrap();
        assert!(matches!(r.tree, BoolNode::Predicate(_)));
    }

    #[test]
    fn e2e_between_view_column_with_utf8_bounds_does_not_fail() {
        let expr = Expr::Between(Between {
            expr: Box::new(col("name")),
            negated: false,
            low: Box::new(lit("a")),
            high: Box::new(lit("z")),
        });
        let r = expr_to_bool_tree(&expr, &view_string_schema()).unwrap();
        // Between may desugar into And of comparisons or stay as-is.
        match r.tree {
            BoolNode::Predicate(_) | BoolNode::And(_) => {}
            other => panic!("expected Predicate or And, got {:?}", other),
        }
    }

    // ── classify_filter ──────────────────────────────────────────────

    fn collector(id: i32) -> BoolNode {
        BoolNode::Collector { annotation_id: id }
    }
    fn dummy_predicate() -> BoolNode {
        // A stand-in Predicate leaf — classify only cares about shape,
        // not expression contents. Build a minimal boolean PhysicalExpr.
        let schema = test_schema();
        let col_idx = schema.index_of("price").unwrap();
        let left: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("price", col_idx));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(0))));
        BoolNode::Predicate(Arc::new(
            datafusion::physical_expr::expressions::BinaryExpr::new(left, Operator::Eq, right),
        ))
    }

    #[test]
    fn classify_no_collectors_is_none() {
        assert_eq!(classify_filter(&dummy_predicate()), FilterClass::None);
        assert_eq!(
            classify_filter(&BoolNode::And(vec![dummy_predicate(), dummy_predicate()])),
            FilterClass::None
        );
    }

    #[test]
    fn classify_bare_collector_is_single() {
        assert_eq!(
            classify_filter(&collector(10)),
            FilterClass::SingleCollector
        );
    }

    #[test]
    fn classify_and_of_collector_and_predicates_is_single() {
        let tree = BoolNode::And(vec![collector(10), dummy_predicate(), dummy_predicate()]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_and_with_two_collectors_is_single() {
        // AND(C, C, P) — all collectors under AND-only path → SingleCollector.
        let tree = BoolNode::And(vec![collector(10), collector(11), dummy_predicate()]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_or_containing_collector_is_tree() {
        let tree = BoolNode::Or(vec![collector(10), dummy_predicate()]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    #[test]
    fn classify_not_of_collector_is_tree() {
        let tree = BoolNode::Not(Box::new(collector(10)));
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    #[test]
    fn classify_and_with_nested_collector_is_tree() {
        let tree = BoolNode::And(vec![
            BoolNode::Or(vec![collector(10), dummy_predicate()]),
            dummy_predicate(),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    // ── Nested AND shapes → SingleCollector ──────────────────────────

    #[test]
    fn classify_nested_and_collector_plus_predicate_is_single() {
        // AND(C₁, AND(C₂, P)) — nested AND, all collectors under AND-only path.
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::And(vec![collector(11), dummy_predicate()]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_deeply_nested_and_is_single() {
        // AND(P, AND(C₁, AND(C₂, AND(C₃, P)))) — depth 4, all AND.
        let tree = BoolNode::And(vec![
            dummy_predicate(),
            BoolNode::And(vec![
                collector(0),
                BoolNode::And(vec![
                    collector(1),
                    BoolNode::And(vec![collector(2), dummy_predicate()]),
                ]),
            ]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_nested_and_only_collectors_is_single() {
        // AND(AND(C₁, C₂), AND(C₃, C₄)) — nested AND of only collectors.
        let tree = BoolNode::And(vec![
            BoolNode::And(vec![collector(0), collector(1)]),
            BoolNode::And(vec![collector(2), collector(3)]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_nested_and_with_or_predicate_is_single() {
        // AND(C, AND(P, OR(P, P))) — OR contains only predicates, no collectors.
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::And(vec![
                dummy_predicate(),
                BoolNode::Or(vec![dummy_predicate(), dummy_predicate()]),
            ]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_nested_and_with_not_predicate_is_single() {
        // AND(C, NOT(P)) — NOT wraps a predicate, not a collector.
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::Not(Box::new(dummy_predicate())),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::SingleCollector);
    }

    #[test]
    fn classify_nested_and_or_containing_collector_is_tree() {
        // AND(C₁, AND(OR(C₂, P), P)) — OR above C₂ → Tree.
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::And(vec![
                BoolNode::Or(vec![collector(11), dummy_predicate()]),
                dummy_predicate(),
            ]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }

    #[test]
    fn classify_nested_and_not_containing_collector_is_tree() {
        // AND(C₁, AND(NOT(C₂), P)) — NOT above C₂ → Tree.
        let tree = BoolNode::And(vec![
            collector(10),
            BoolNode::And(vec![
                BoolNode::Not(Box::new(collector(11))),
                dummy_predicate(),
            ]),
        ]);
        assert_eq!(classify_filter(&tree), FilterClass::Tree);
    }
}

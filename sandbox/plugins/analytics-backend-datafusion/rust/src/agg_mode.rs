/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Aggregate mode forcing utilities for partial/final distributed execution.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;

/// Returns the default physical optimizer rules with `CombinePartialFinalAggregate` removed.
/// This keeps `Final(Partial(...))` pairs intact so `force_aggregate_mode` can cleanly
/// strip one half for distributed execution.
pub(crate) fn physical_optimizer_rules_without_combine() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let combine_name = CombinePartialFinalAggregate::new().name().to_string();
    PhysicalOptimizer::default()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != combine_name)
        .collect()
}

/// Applies aggregate mode forcing if mode != 0.
/// 0 = default (no forcing), 1 = partial, 2 = final.
pub(crate) fn apply_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    mode: i32,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    match mode {
        0 => Ok(plan),
        1 => force_aggregate_mode(plan, AggregateMode::Partial),
        2 => force_aggregate_mode(plan, AggregateMode::Final),
        _ => Err(DataFusionError::Execution(format!("Unknown aggregate mode: {mode}"))),
    }
}

/// Walks a physical plan and forces the target aggregation mode on scalar aggregates
/// (no group-by keys). With `CombinePartialFinalAggregate` disabled, the plan is always
/// `Final(Partial(...))`:
/// - Partial: strips the Final, keeping only the Partial (shard emits intermediate state)
/// - Final: strips the Partial, connecting Final directly to the streaming table input
fn force_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    target_mode: AggregateMode,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if agg.group_expr().is_empty() {
            match target_mode {
                AggregateMode::Partial => {
                    // Strip Final → recurse into its input to find and keep the Partial
                    if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
                        return force_aggregate_mode(Arc::clone(agg.input()), target_mode);
                    }
                    // Already Partial — return as-is
                    if matches!(agg.mode(), AggregateMode::Partial) {
                        return Ok(plan);
                    }
                }
                AggregateMode::Final => {
                    // Strip Partial → connect Final directly to Partial's input
                    if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
                        if let Some(partial_input) = find_partial_input(agg.input()) {
                            let coalesced = Arc::new(CoalescePartitionsExec::new(partial_input));
                            return Ok(plan.with_new_children(vec![coalesced])?);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    // Recurse into children
    let new_children: datafusion_common::Result<Vec<_>> = plan
        .children()
        .into_iter()
        .map(|c| force_aggregate_mode(Arc::clone(c), target_mode))
        .collect();
    plan.with_new_children(new_children?)
}

/// Walks through single-child passthrough nodes to find a Partial AggregateExec, returns its input.
fn find_partial_input(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Partial) {
            return Some(Arc::clone(agg.input()));
        }
    }
    if plan.children().len() == 1 {
        return find_partial_input(plan.children()[0]);
    }
    None
}

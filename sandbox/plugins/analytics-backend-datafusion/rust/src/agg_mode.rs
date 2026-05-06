/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Aggregate mode forcing utilities for partial/final distributed execution.

use std::sync::Arc;

/// Aggregate execution mode for distributed partial/final execution.
#[derive(Clone, Copy, Default, PartialEq)]
#[derive(Debug)]
pub(crate) enum Mode {
    /// No mode forcing — plain scan or full aggregation.
    #[default]
    Default,
    /// Shard emits intermediate aggregate state.
    Partial,
    /// Coordinator merges partial state into final result.
    Final,
}

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

/// Applies aggregate mode forcing based on the configured mode.
pub(crate) fn apply_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    mode: Mode,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    match mode {
        Mode::Default => Ok(plan),
        Mode::Partial => force_aggregate_mode(plan, AggregateMode::Partial),
        Mode::Final => force_aggregate_mode(plan, AggregateMode::Final),
    }
}

/// Walks a physical plan and forces the target aggregation mode.
/// With `CombinePartialFinalAggregate` disabled, the plan is always
/// `Final(Partial(...))`  or `FinalPartitioned(Partial(...))`:
/// - Partial: strips the Final, keeping only the Partial (shard emits intermediate state)
/// - Final: strips the Partial, connecting Final directly to the streaming table input
fn force_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    target_mode: AggregateMode,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        match target_mode {
            AggregateMode::Partial => {
                if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
                    // Walk through intermediate nodes (e.g. RepartitionExec) to find Partial
                    return find_partial_agg(Arc::clone(agg.input()));
                }
                if matches!(agg.mode(), AggregateMode::Partial) {
                    return Ok(plan);
                }
            }
            AggregateMode::Final => {
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
    let new_children: datafusion_common::Result<Vec<_>> = plan
        .children()
        .into_iter()
        .map(|c| force_aggregate_mode(Arc::clone(c), target_mode))
        .collect();
    plan.with_new_children(new_children?)
}

/// Walks through single-child nodes to find and return a Partial AggregateExec directly.
fn find_partial_agg(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Partial) {
            return Ok(plan);
        }
    }
    if plan.children().len() == 1 {
        return find_partial_agg(Arc::clone(plan.children()[0]));
    }
    Ok(plan)
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

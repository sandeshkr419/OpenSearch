/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use datafusion::{
    common::DataFusionError,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    execution::context::SessionContext,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::SessionStateBuilder,
    physical_plan::execute_stream,
    prelude::*,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use log::error;
use object_store::ObjectMeta;
use prost::Message;
use substrait::proto::Plan;

use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::api::DataFusionRuntime;
use crate::session_context::SessionContextHandle;

/// Execute a vanilla parquet query: substrait plan → DataFusion → CrossRtStream.
/// File access goes through DataFusion's registered object store.
///
/// Deprecated: Production now uses the decomposed `create_session_context` +
/// `execute_with_context` path (via `api::execute_query`).
/// TODO: Remove this function and migrate benchmarks to the decomposed path.
/// Retained only for benchmarks. TODO: migrate benchmarks and remove.
pub async fn execute_query(
    table_path: ListingTableUrl,
    object_metas: Arc<Vec<ObjectMeta>>,
    table_name: String,
    plan_bytes: Vec<u8>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    // Per-query memory pool, or None when context_id is 0 (tracking disabled).
    // Not all query flows pass a context_id yet; this fallback allows queries
    // to execute using the global pool. Can be made required once all flows
    // wire up context_id correctly.
    query_memory_pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
    query_config: &crate::datafusion_query_config::DatafusionQueryConfig,
) -> Result<i64, DataFusionError> {
    // Pre-populate the list-files cache so DataFusion doesn't re-list the directory
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    let table_scoped_path = datafusion::execution::cache::TableScopedPath {
        table: None,
        path: table_path.prefix().clone(),
    };
    list_file_cache.put(&table_scoped_path, object_metas);

    // Build a per-query RuntimeEnv sharing the global memory pool + caches,
    // but with a fresh list-files cache for this query's shard files.
    let mut runtime_env_builder = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        );

    // If a per-query memory pool is provided, set it on the same builder.
    // The per-query pool wraps the global pool, so global limits are still enforced.
    if let Some(pool) = query_memory_pool {
        runtime_env_builder = runtime_env_builder.with_memory_pool(pool);
    }

    let runtime_env = runtime_env_builder
        .build()
        .map_err(|e| {
            error!("Failed to build runtime env: {}", e);
            e
        })?;

    // Build a fresh session state per query. TODO : Tune this during planning per query
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = query_config.parquet_pushdown_filters;
    config.options_mut().execution.target_partitions = query_config.target_partitions;
    config.options_mut().execution.batch_size = query_config.batch_size;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Register table via ListingTable — all IO goes through object store
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await
        .map_err(|e| {
            error!("Failed to infer schema: {}", e);
            e
        })?;

    let table_config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = Arc::new(ListingTable::try_new(table_config).map_err(|e| {
        error!("Failed to create listing table: {}", e);
        e
    })?);

    ctx.register_table(&table_name, provider).map_err(|e| {
        error!("Failed to register table: {}", e);
        e
    })?;

    // Decode substrait → logical plan → physical plan → stream
    let substrait_plan = Plan::decode(plan_bytes.as_slice()).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;

    let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;

    let df_stream = execute_stream(physical_plan, ctx.task_ctx()).map_err(|e| {
        error!("Failed to create execution stream: {}", e);
        e
    })?;

    // Wrap in CrossRtStream — CPU work runs on DedicatedExecutor
    let cross_rt_stream =
        CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Ok(Box::into_raw(Box::new(wrapped)) as i64)
}

/// Executes a Substrait plan against a pre-configured SessionContext.
/// Consumes the handle — SessionContext lifetime is tied to the returned stream.
///
/// `mode`: 0 = default (no forcing), 1 = partial, 2 = final.
pub async unsafe fn execute_with_context(
    session_ctx_ptr: i64,
    plan_bytes: &[u8],
    cpu_executor: DedicatedExecutor,
    mode: i32,
) -> Result<i64, DataFusionError> {
    let handle = *Box::from_raw(session_ctx_ptr as *mut SessionContextHandle);

    let substrait_plan = Plan::decode(plan_bytes).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;

    let logical_plan = from_substrait_plan(&handle.ctx.state(), &substrait_plan).await?;
    let dataframe = handle.ctx.execute_logical_plan(logical_plan).await?;
    let mut physical_plan = dataframe.create_physical_plan().await?;

    physical_plan = apply_aggregate_mode(physical_plan, mode)?;

    let df_stream = execute_stream(physical_plan, handle.ctx.task_ctx()).map_err(|e| {
        error!("execute_with_context: failed to create stream: {}", e);
        e
    })?;

    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    let stream_handle = crate::api::QueryStreamHandle::new(wrapped, handle.query_context);
    Ok(Box::into_raw(Box::new(stream_handle)) as i64)
}

/// Applies aggregate mode forcing if mode != 0.
pub fn apply_aggregate_mode(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    mode: i32,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, DataFusionError> {
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
    match mode {
        0 => Ok(plan),
        1 => force_aggregate_mode(plan, AggregateMode::Partial),
        2 => force_aggregate_mode(plan, AggregateMode::Final),
        _ => Err(DataFusionError::Execution(format!("Unknown aggregate mode: {mode}"))),
    }
}

/// Walks a physical plan and restructures `Final(Partial(...))` pairs for scalar aggregates
/// (no group-by keys) to force the target aggregation mode:
/// - `Partial`: strips the Final, keeping only the Partial so the shard emits intermediate state
/// - `Final`: strips the Partial, connecting Final directly to the streaming table input
///
/// Group-by aggregates are left unchanged.
fn force_aggregate_mode(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    target_mode: datafusion::physical_plan::aggregates::AggregateMode,
) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, DataFusionError> {
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};

    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if agg.group_expr().is_empty() {
            match target_mode {
                AggregateMode::Partial => {
                    if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
                        return force_aggregate_mode(Arc::clone(agg.input()), target_mode);
                    }
                    if matches!(agg.mode(), AggregateMode::Single | AggregateMode::SinglePartitioned) {
                        let partial = AggregateExec::try_new(
                            AggregateMode::Partial,
                            agg.group_expr().clone(),
                            agg.aggr_expr().to_vec(),
                            agg.filter_expr().to_vec(),
                            Arc::clone(agg.input()),
                            agg.input_schema().clone(),
                        )?;
                        return Ok(Arc::new(partial));
                    }
                }
                AggregateMode::Final => {
                    if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
                        if let Some(partial_input) = find_partial_input(agg.input()) {
                            let coalesced = Arc::new(
                                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(partial_input),
                            );
                            return Ok(plan.with_new_children(vec![coalesced])?);
                        }
                    }
                    if matches!(agg.mode(), AggregateMode::Single | AggregateMode::SinglePartitioned) {
                        let coalesced = Arc::new(
                            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(Arc::clone(agg.input())),
                        );
                        return Ok(plan.with_new_children(vec![coalesced])?);
                    }
                }
                _ => {}
            }
        }
    }
    let new_children: datafusion_common::Result<Vec<_>> = plan
        .children()
        .into_iter()
        .map(|c| force_aggregate_mode(Arc::clone(c), target_mode))
        .collect();
    plan.with_new_children(new_children?)
}

/// Walks through single-child passthrough nodes to find a Partial AggregateExec, returns its input.
fn find_partial_input(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
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

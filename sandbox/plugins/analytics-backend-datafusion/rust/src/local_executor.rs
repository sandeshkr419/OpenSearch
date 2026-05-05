/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Coordinator-reduce local execution.
//!
//! A [`LocalSession`] holds a DataFusion [`SessionContext`] configured to share
//! the caller-supplied [`RuntimeEnv`] (and therefore its memory pool) with the
//! rest of the node. The session is the Rust-side counterpart of
//! `DatafusionReduceSink` on the Java side:
//!
//! 1. For each declared stage input, [`LocalSession::register_partition`]
//!    creates a [`PartitionStreamSender`] / [`PartitionStreamReceiver`] pair,
//!    wraps the receiver in a [`SingleReceiverPartition`], and registers it as
//!    a [`StreamingTable`] on the session under the input id.
//! 2. [`LocalSession::execute_substrait`] decodes a Substrait plan against the
//!    session (its table references resolve to the streaming tables) and hands
//!    back a [`SendableRecordBatchStream`] the bridge layer can drain.
//!
//! The session has no knowledge of the FFM bridge; it is exposed to Java via a
//! raw `Box::into_raw` pointer managed in `api.rs`, matching the lifecycle
//! model used by `DataFusionRuntime` / `ShardView` / `QueryStreamHandle`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
pub use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion_physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use prost::Message;
use substrait::proto::Plan;

use crate::partition_stream::{channel, PartitionStreamSender, SingleReceiverPartition};

/// Coordinator-reduce DataFusion session.
///
/// Owns a [`SessionContext`] that reuses the caller's [`RuntimeEnv`] so memory
/// accounting shares the node-wide pool. One session corresponds to one reduce
/// stage; it holds the streaming inputs registered by
/// [`Self::register_partition`] and is drained exactly once via
/// [`Self::execute_substrait`].
pub struct LocalSession {
    ctx: SessionContext,
}

impl LocalSession {
    /// Builds a session whose `SessionContext` reuses the given [`RuntimeEnv`].
    ///
    /// The runtime's memory pool, disk manager, and caches are inherited —
    /// every batch consumed or produced by this session counts against the
    /// same limits as the shard-scan path.
    pub fn new(runtime_env: &RuntimeEnv) -> Self {
        // Cheaply clone the env so the session owns a handle independent of
        // the caller. `RuntimeEnv` internally holds `Arc`s — this is a
        // lightweight clone, not a deep copy of the pool or disk manager.
        let runtime_env = Arc::new(runtime_env.clone());
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_physical_optimizer_rules(physical_optimizer_rules_without_combine())
            .build();
        let ctx = SessionContext::new_with_state(state);
        Self { ctx }
    }

    /// Registers a streaming input on the session under `name` and returns the
    /// producer side of the channel.
    ///
    /// The receiver is wrapped in a [`SingleReceiverPartition`] and registered
    /// as a [`StreamingTable`]; Substrait plans executed through
    /// [`Self::execute_substrait`] resolve table references named `name` to
    /// this streaming table. The caller pushes `RecordBatch`es into the
    /// returned [`PartitionStreamSender`] via
    /// [`PartitionStreamSender::send_blocking`].
    pub fn register_partition(
        &mut self,
        name: &str,
        schema: SchemaRef,
    ) -> Result<PartitionStreamSender, DataFusionError> {
        let (sender, receiver) = channel(Arc::clone(&schema));
        let partition: Arc<dyn PartitionStream> =
            Arc::new(SingleReceiverPartition::new(receiver));
        let table = StreamingTable::try_new(schema, vec![partition])?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to register streaming table '{}': {}",
                    name, e
                ))
            })?;
        Ok(sender)
    }

    /// Registers an in-memory input on the session under `name`, holding all
    /// `batches` in a single [`MemTable`] partition.
    ///
    /// Unlike [`Self::register_partition`], this method does not return a
    /// channel sender — the batches are fully materialized in the table. Used
    /// by the memtable variant of the coordinator-reduce sink, which buffers
    /// shard responses in Java and hands them across in one call.
    pub fn register_memtable(
        &mut self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DataFusionError> {
        let table = MemTable::try_new(schema, vec![batches])?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to register memtable '{}': {}",
                    name, e
                ))
            })?;
        Ok(())
    }

    /// Decodes a Substrait plan against the session and returns the resulting
    /// stream.
    ///
    /// Table references in the plan resolve through the session's registered
    /// streaming tables, so input batches pushed into
    /// [`PartitionStreamSender`]s flow naturally into the DataFusion physical
    /// plan. The returned stream is hot — polling it drives both the reduce
    /// computation and the consumption of the streaming inputs.
    pub async fn execute_substrait(
        &self,
        bytes: &[u8],
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let plan = Plan::decode(bytes).map_err(|e| {
            DataFusionError::Execution(format!("Failed to decode Substrait plan: {}", e))
        })?;
        let logical_plan = from_substrait_plan(&self.ctx.state(), &plan).await?;
        self.ctx
            .execute_logical_plan(logical_plan)
            .await?
            .execute_stream()
            .await
    }

    /// Executes a Substrait plan in partial-aggregate mode.
    ///
    pub async fn execute_partial_substrait(
        &self,
        bytes: &[u8],
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.execute_substrait_with_agg_mode(bytes, AggregateMode::Partial).await
    }

    pub async fn execute_final_substrait(
        &self,
        bytes: &[u8],
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.execute_substrait_with_agg_mode(bytes, AggregateMode::Final).await
    }

    async fn execute_substrait_with_agg_mode(
        &self,
        bytes: &[u8],
        mode: AggregateMode,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let plan = Plan::decode(bytes).map_err(|e| {
            DataFusionError::Execution(format!("Failed to decode Substrait plan: {}", e))
        })?;
        let logical_plan = from_substrait_plan(&self.ctx.state(), &plan).await?;
        let df = self.ctx.execute_logical_plan(logical_plan).await?;
        let physical_plan = df.create_physical_plan().await?;
        let physical_plan = force_aggregate_mode(physical_plan, mode)?;
        let task_ctx = self.ctx.task_ctx();
        datafusion::physical_plan::execute_stream(physical_plan, task_ctx)
    }

    /// Returns the memory pool the session's `RuntimeEnv` was built with.
    ///
    /// Used by the bridge layer to seed a per-query tracking context so
    /// reduce-stage allocations count against the same pool as the shard-scan
    /// path.
    pub fn memory_pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.ctx.runtime_env().memory_pool)
    }

    pub fn state(&self) -> datafusion::execution::SessionState {
        self.ctx.state()
    }

    pub async fn execute_logical_plan(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
    ) -> datafusion_common::Result<datafusion::dataframe::DataFrame> {
        self.ctx.execute_logical_plan(plan).await
    }

    pub fn task_ctx(&self) -> Arc<datafusion::execution::TaskContext> {
        self.ctx.task_ctx()
    }
}

/// Walks a physical plan and restructures `Final(Partial(...))` pairs for scalar aggregates
/// (no group-by keys) to force the target aggregation mode:
/// - `Partial`: strips the Final, keeping only the Partial so the shard emits intermediate state
/// - `Final`: strips the Partial, connecting Final directly to the streaming table input
///
/// Group-by aggregates are left unchanged — DataFusion's native `Final(Partial(...))` structure
/// correctly re-groups partial states per key.
pub fn force_aggregate_mode(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    target_mode: AggregateMode,
) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        // DataFusion's physical planner creates Final(Partial(...)) pairs.
        // For scalar aggregates (no group-by), we force the target mode:
        // - Partial: strip the Final, keep only the Partial (shard emits intermediate state)
        // - Final: strip the Partial, connect Final directly to its input (coordinator merges)
        if agg.group_expr().is_empty() {
            match target_mode {
                AggregateMode::Partial => {
                    // Strip Final → recurse into its input (the Partial)
                    if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
                        return force_aggregate_mode(Arc::clone(agg.input()), target_mode);
                    }
                    // Single mode: replace with Partial so the shard emits intermediate state.
                    if matches!(agg.mode(), AggregateMode::Single | AggregateMode::SinglePartitioned) {
                        let partial = datafusion::physical_plan::aggregates::AggregateExec::try_new(
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
                    // Strip Partial → connect Final directly to Partial's input
                    if matches!(agg.mode(), AggregateMode::Final | AggregateMode::FinalPartitioned) {
                        if let Some(partial_input) = find_partial_input(agg.input()) {
                            let coalesced = Arc::new(
                                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(partial_input)
                            );
                            return Ok(plan.with_new_children(vec![coalesced])?);
                        }
                    }
                    // Single mode: connect directly to streaming table input.
                    if matches!(agg.mode(), AggregateMode::Single | AggregateMode::SinglePartitioned) {
                        let coalesced = Arc::new(
                            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(Arc::clone(agg.input()))
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

/// Walks through single-child passthrough nodes (CoalescePartitions, Repartition)
/// to find a Partial AggregateExec, then returns its input.
fn find_partial_input(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
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

/// Returns the default physical optimizer rules with [`CombinePartialFinalAggregate`] removed.
/// That rule recombines partial+final aggregates into a single pass, undoing the distributed
/// split. Disabling it keeps `Final(Partial(...))` pairs intact for `force_aggregate_mode`.
pub fn physical_optimizer_rules_without_combine(
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let combine_name = CombinePartialFinalAggregate::new().name().to_string();
    PhysicalOptimizer::default()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != combine_name)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use futures::StreamExt;
    use tokio::runtime::Handle;

    fn test_runtime_env() -> RuntimeEnv {
        RuntimeEnvBuilder::new()
            .build()
            .expect("runtime env builds")
    }

    fn i64_schema(column: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, false)]))
    }

    fn i64_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("batch builds")
    }

    #[tokio::test]
    async fn register_partition_makes_table_resolvable() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");
        let _sender = session
            .register_partition("input-0", Arc::clone(&schema))
            .expect("register succeeds");

        // A trivial `SELECT * FROM "input-0"` proves the table resolves.
        let df = session.ctx.sql("SELECT x FROM \"input-0\"").await.expect("sql parses");
        assert_eq!(df.schema().fields().len(), 1);
    }

    #[tokio::test]
    async fn execute_substrait_sums_streaming_input() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");
        let sender = session
            .register_partition("input-0", Arc::clone(&schema))
            .expect("register succeeds");

        // Build the Substrait bytes from a SQL-built logical plan against a
        // matching session — the plan only references `input-0`, so it is
        // portable onto our real session.
        let substrait_bytes = {
            let env = test_runtime_env();
            let mut producer = LocalSession::new(&env);
            let _unused = producer
                .register_partition("input-0", Arc::clone(&schema))
                .expect("producer register");
            let df = producer
                .ctx
                .sql("SELECT SUM(x) AS total FROM \"input-0\"")
                .await
                .expect("sum parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state())
                .expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        // Push three batches totaling 45 = 1+2+3+4+5+6+7+8+9, then close.
        let producer_schema = Arc::clone(&schema);
        let handle = Handle::current();
        let producer = std::thread::spawn(move || {
            for chunk in &[vec![1i64, 2, 3], vec![4, 5, 6], vec![7, 8, 9]] {
                sender
                    .send_blocking(Ok(i64_batch(&producer_schema, chunk)), &handle)
                    .expect("send");
            }
            drop(sender); // EOF
        });

        let mut stream = session
            .execute_substrait(&substrait_bytes)
            .await
            .expect("execute");

        let mut total: i64 = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 col");
            for i in 0..col.len() {
                total += col.value(i);
            }
        }
        producer.join().expect("producer thread");
        assert_eq!(total, 45);
    }

    #[tokio::test]
    async fn execute_substrait_sums_memtable_input() {
        let env = test_runtime_env();
        let mut session = LocalSession::new(&env);
        let schema = i64_schema("x");

        let batches = vec![
            i64_batch(&schema, &[1, 2, 3]),
            i64_batch(&schema, &[4, 5, 6]),
            i64_batch(&schema, &[7, 8, 9]),
        ];
        session
            .register_memtable("input-0", Arc::clone(&schema), batches)
            .expect("register memtable");

        // Build the Substrait bytes from a SQL-built logical plan against a
        // matching session — the plan only references `input-0`, so it is
        // portable onto our real session.
        let substrait_bytes = {
            let env = test_runtime_env();
            let mut producer = LocalSession::new(&env);
            producer
                .register_memtable("input-0", Arc::clone(&schema), vec![])
                .expect("producer register");
            let df = producer
                .ctx
                .sql("SELECT SUM(x) AS total FROM \"input-0\"")
                .await
                .expect("sum parses");
            let plan = df.logical_plan().clone();
            let substrait = to_substrait_plan(&plan, &producer.ctx.state())
                .expect("to_substrait");
            let mut buf = Vec::new();
            substrait.encode(&mut buf).expect("encode");
            buf
        };

        let mut stream = session
            .execute_substrait(&substrait_bytes)
            .await
            .expect("execute");

        let mut total: i64 = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 col");
            for i in 0..col.len() {
                total += col.value(i);
            }
        }
        assert_eq!(total, 45);
    }
}

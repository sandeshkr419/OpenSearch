/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use std::ptr::addr_of_mut;
use std::sync::Arc;
use std::time::Instant;
use arrow::datatypes::Schema;
use arrow_schema::{Field, DataType};

// arrow imports
use arrow_array::{Array, BinaryArray, StructArray};
use arrow_schema::ArrowError;

// Local modules
mod util;
mod row_id_optimizer;
mod listing_table;

// ++ DataFusion Imports ++
use datafusion::error::{DataFusionError, Result};
use datafusion_expr::expr::Alias;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::col as phys_col;
use datafusion_functions_aggregate::approx_distinct::ApproxDistinct;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion_datasource::source::DataSourceExec;
use datafusion::catalog::TableProvider;
// use datafusion::physical_plan::display::DisplayableExecutionPlan;
// use datafusion::physical_plan::planner::PhysicalPlanner;

use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::util::{create_object_meta_from_filenames, parse_string_arr, set_object_result_error, set_object_result_ok};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr}; // Import the builder AND the struct it builds
use datafusion_expr::AggregateUDF;
use datafusion::functions_aggregate::count::Count;
// use datafusion::physical_expr::aggregates::AggregateExpr;

mod partial_agg_optimizer;
use crate::partial_agg_optimizer::PartialAggregationOptimizer;

use datafusion::execution::context::SessionContext;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::DefaultListFilesCache;
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionConfig;
use datafusion::DATAFUSION_VERSION;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::logical_expr::utils as logical_utils;

use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion_common::{tree_node::{TreeNode, Transformed}};
use datafusion_expr::{
    logical_plan::LogicalPlan,
    logical_plan::Aggregate,
    Expr,
    expr::{AggregateFunction as AggFnExpr}
};
use datafusion::functions_aggregate::expr_fn as agg_expr_fn;
use datafusion::optimizer::Analyzer;

use futures::TryStreamExt;
use jni::objects::{JObjectArray, JString};
use object_store::ObjectMeta;
use prost::Message;
use tokio::runtime::Runtime;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};


/// Create a new DataFusion session context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}

/// Close and cleanup a DataFusion context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
}

/// Get version information
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersionInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(r#"{{"version": "{}", "codecs": ["CsvDataSourceCodec"]}}"#, DATAFUSION_VERSION);
    env.new_string(version_info).expect("Couldn't create Java string").as_raw()
}

pub fn extract_partial_aggregate(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    // Try downcasting to AggregateExec
    if let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
        println!(
            "[PartialAggOptimizer] Found AggregateExec: mode={:?}, group_expr={:?}, aggr_expr={:?}",
            agg_exec.mode(),
            agg_exec.group_expr(),
            agg_exec.aggr_expr()
        );

        // Recursively fix input first
        let new_input = extract_partial_aggregate(agg_exec.input().clone());

        // Process aggregate expressions
        let mut new_aggr_exprs: Vec<Arc<AggregateFunctionExpr>> = Vec::new();

        for expr in agg_exec.aggr_expr() {
            let func_name = expr.fun().name().to_uppercase();
            println!(
                "[PartialAggOptimizer] Processing aggregate function: {}",
                func_name
            );

            // Replace COUNT with ApproxDistinct HLL for partial aggregation
            if func_name == "COUNT" {
                println!("[PartialAggOptimizer] Replacing COUNT with ApproxDistinct HLL UDAF");

                // Create ApproxDistinct UDAF
                let agg_udf = Arc::new(AggregateUDF::new_from_impl(ApproxDistinct::new()));

                // Keep a distinct name for partial HLL
                let alias_name = format!("{}[hll_registers]", expr.name());

                // HLL produces Binary type
                let field = Field::new(&alias_name, DataType::Binary, false);
                let schema = Arc::new(Schema::new(vec![field.clone()]));

                // Build new AggregateFunctionExpr
                let new_expr_result = AggregateExprBuilder::new(agg_udf.clone(), expr.expressions().to_vec())
                    .alias(alias_name.clone())
                    .schema(schema.clone())
                    .build();

                match new_expr_result {
                    Ok(new_expr) => {
                        println!(
                            "[PartialAggOptimizer] Created new AggregateFunctionExpr: {:?}",
                            new_expr
                        );
                        new_aggr_exprs.push(Arc::new(new_expr));
                    }
                    Err(e) => {
                        println!(
                            "[PartialAggOptimizer] FAILED to build new HLL agg expr: {}. Keeping original.",
                            e
                        );
                        new_aggr_exprs.push(expr.clone());
                    }
                }
            } else {
                // Keep other aggregates as-is
                new_aggr_exprs.push(expr.clone());
            }
        }

        // Build new PhysicalGroupBy for group expressions
        let new_group_exprs = agg_exec.group_expr().clone();
        println!(
            "[PartialAggOptimizer] Group expressions: {:?}",
            new_group_exprs
        );

        // Build schema from new expressions
        let new_schema = Arc::new(Schema::new(
            new_aggr_exprs
                .iter()
                .map(|e| (*e.field()).clone())
                .collect::<Vec<Field>>(),
        ));
        println!("[PartialAggOptimizer] New schema: {:?}", new_schema);

        // Create the new AggregateExec in Partial mode
        let new_agg_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            new_group_exprs,
            new_aggr_exprs,
            agg_exec.filter_expr().to_vec(),
            new_input,
            new_schema,
        )
            .expect("[PartialAggOptimizer] Failed to create AggregateExec");

        println!(
            "[PartialAggOptimizer] Created new AggregateExec in Partial mode: {:?}",
            new_agg_exec
        );

        Arc::new(new_agg_exec)
    } else {
        // Recurse into children of non-aggregate nodes
        println!(
            "[PartialAggOptimizer] Recurse into children of node: {}",
            plan.name()
        );

        let new_children = plan
            .children()
            .into_iter()
            .map(|child| extract_partial_aggregate(child.clone()))
            .collect::<Vec<_>>();

        let plan_with_children = plan.clone().with_new_children(new_children);

        match plan_with_children {
            Ok(new_plan) => new_plan,
            Err(e) => {
                println!(
                    "[PartialAggOptimizer] ERROR in with_new_children for {}: {}. Returning original.",
                    plan.name(),
                    e
                );
                plan
            }
        }
    }
}


/// Get version information (legacy method name)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    env.new_string(DATAFUSION_VERSION).expect("Couldn't create Java string").as_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createTokioRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let rt = Runtime::new().unwrap();
    let ctx = Box::into_raw(Box::new(rt)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let runtime_env = RuntimeEnvBuilder::default().build().unwrap();
    /**
    // We can copy global runtime to local runtime - file statistics cache, and most of the things
    // will be shared across session contexts. But list files cache will be specific to session
    // context

    let fsCache = runtimeEnv.clone().cache_manager.get_file_statistic_cache().unwrap();
    let localCacheManagerConfig = CacheManagerConfig::default().with_files_statistics_cache(Option::from(fsCache));
    let localCacheManager = CacheManager::try_new(&localCacheManagerConfig);
    let localRuntimeEnv = RuntimeEnvBuilder::new()
        .with_cache_manager(localCacheManagerConfig)
        .with_disk_manager(DiskManagerConfig::new_existing(runtimeEnv.disk_manager))
        .with_memory_pool(runtimeEnv.memory_pool)
        .with_object_store_registry(runtimeEnv.object_store_registry)
        .build();
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    **/

    let ctx = Box::into_raw(Box::new(runtime_env)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    let runtimeEnv = unsafe { &mut *(runtime_id as *mut RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtimeEnv.clone()));
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
}


#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray
) -> jlong {

    let table_path: String = env.get_string(&table_path).expect("Couldn't get java string!").into();
    let files: Vec<String> = parse_string_arr(&mut env, files).expect("Expected list of files");
    let files_meta = create_object_meta_from_filenames(&table_path, files);

    let table_path = ListingTableUrl::parse(table_path).unwrap();
    let shard_view = ShardView::new(table_path, files_meta);
    Box::into_raw(Box::new(shard_view)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_destroyReader(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong
)  {
    let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
}

pub struct ShardView {
    table_path: ListingTableUrl,
    files_meta: Arc<Vec<ObjectMeta>>
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_meta: Vec<ObjectMeta>) -> Self {
        let files_meta = Arc::new(files_meta);
        ShardView {
            table_path,
            files_meta
        }
    }

    pub fn table_path(&self) -> ListingTableUrl {
        self.table_path.clone()
    }

    pub fn files_meta(&self) -> Arc<Vec<ObjectMeta>> {
        self.files_meta.clone()
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_executeSubstraitQuery(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
    tokio_runtime_env_ptr: jlong,
    // callback: JObject,
) -> jlong {
    let overall = Instant::now();
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime_ptr = unsafe { &*(tokio_runtime_env_ptr as *const Runtime)};
    let table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();

    println!("Table path: {}", table_path);
    println!("Files: {:?}", files_meta);

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), files_meta);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
            .with_list_files_cache(Some(list_file_cache.clone()))
        ).build().unwrap();

    // TODO: get config from CSV DataFormat
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.target_partitions = 1;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(PartialAggregationOptimizer))
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet"); // TODO: take this as parameter
    // .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int32)]); // TODO: enable only for query phase

    // Ideally the executor will give this
    runtime_ptr.block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();


        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table(table_name, provider)
            .expect("Failed to attach the Table");

    });

    let start = Instant::now();
    // TODO : how to close ctx ?
    // Convert Java byte array to Rust Vec<u8>
    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to convert plan bytes: {}", e);
            env.throw_new("java/lang/Exception", error_msg);
            return 0;
        }
    };

    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => {
            // println!("SUBSTRAIT rust: Decoding is successful, Plan has {} relations", plan.relations.len());
            plan
        },
        Err(e) => {
            return 0;
        }
    };


    //let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    runtime_ptr.block_on(async {

        let logical_plan = match from_substrait_plan(&ctx.state(), &substrait_plan).await {
            Ok(plan) => {
                // println!("SUBSTRAIT Rust: LogicalPlan: {:?}", plan);
                let duration = start.elapsed();
                println!("Rust: Substrait decoding time in milliseconds: {}", duration.as_millis());
                plan
            },
            Err(e) => {
                println!("SUBSTRAIT Rust: Failed to convert Substrait plan: {}", e);
                return 0;
            }
        };

        let dataframe = ctx.execute_logical_plan(logical_plan).await.expect("Failed to execute logical plan");
        let physical_plan = dataframe.clone().create_physical_plan().await.unwrap();
        println!("Physical Plan:\n{}", datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true));

        let stream = match dataframe.execute_stream().await {
            Ok(stream) => { stream }
            Err(e) => {
                let error_msg = format!("Failed to execute stream: {}", e);
                println!("{}", error_msg);
                env.throw_new("java/lang/Exception", error_msg);
                return 0;
            }
        };
        let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;
        // println!("The memory used currently right now: {:?}", jemalloc_stats::refresh_allocated());
        let duration1 = overall.elapsed();
        println!("Rust: Overall query setup time in milliseconds: {}", duration1.as_millis());

        // set_projections(env, projections, callback);
        stream_ptr
    })
}

pub fn rewrite_count_to_approx_distinct(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> Result<LogicalPlan, DataFusionError> {
    // 1) Walk the logical plan and rewrite Aggregate nodes
    let transformed = plan.clone().transform_up(&|node| match node {
        LogicalPlan::Aggregate(agg) => {
            let new_aggr_exprs: Result<Vec<Expr>, DataFusionError> = agg
                .aggr_expr
                .iter()
                .map(|expr| match expr {
                    Expr::AggregateFunction(af) if af.func.name() == "count" => {
                        // Build AggregateUDF for approx_distinct
                        let udf_impl = ApproxDistinct::new(); // Your implementation
                        let udf = Arc::new(AggregateUDF::new_from_impl(udf_impl));

                        // Extract params
                        let params = &af.params;

                        // Create new AggregateFunction
                        let new_func = AggregateFunction::new_udf(
                            udf.clone(),
                            params.args.clone(),
                            params.distinct,
                            params.filter.clone(),
                            params.order_by.clone(),
                            params.null_treatment.clone(),
                        );

                        // Rename column to avoid schema mismatch
                        let arg_name = match params.args.get(0) {
                            Some(Expr::Column(c)) => c.name.clone(),
                            _ => "col".to_string(),
                        };
                        Ok(Expr::Alias(Alias {
                            expr: Box::new(Expr::AggregateFunction(new_func)),
                            name: format!("approx_distinct({})", arg_name),
                            relation: None,
                            metadata: None,
                        }))
                    }
                    _ => Ok(expr.clone()),
                })
                .collect();

            let new_agg = Aggregate::try_new(
                agg.input.clone(),
                agg.group_expr.clone(),
                new_aggr_exprs?,
            )?;
            Ok(Transformed::yes(LogicalPlan::Aggregate(new_agg)))
        }
        _ => Ok(Transformed::no(node)),
    })?;

    let rewritten = transformed.data;

    // 2) Re-run analyzer to recompute schemas / aliases and resolve coercions
    let analyzer = Analyzer::new();

    // execute_and_check takes (LogicalPlan, &ConfigOptions, observer)
    // observer is FnMut(&LogicalPlan, &dyn AnalyzerRule) — pass a no-op closure
    let analyzed = analyzer.execute_and_check(
        rewritten.clone(),
        ctx.state().config_options(),
        |_, _| {}, // no-op observer
    )?;

    Ok(analyzed)
}

// If we need to create session context separately
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_nativeCreateSessionContext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    shard_view_ptr: jlong,
    global_runtime_env_ptr: jlong,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();

    // Will use it once the global RunTime is defined
    // let runtime_arc = unsafe {
    //     let boxed = &*(runtime_env_ptr as *const Pin<Arc<RuntimeEnv>>);
    //     (**boxed).clone()
    // };

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), files_meta);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
            .with_list_files_cache(Some(list_file_cache))).build().unwrap();



    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env));


    // Create default parquet options
    let file_format = CsvFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".csv");


    // let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    let mut session_context_ptr = 0;

    // Ideally the executor will give this
    Runtime::new().expect("Failed to create Tokio Runtime").block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();


        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table(shard_id, provider)
            .expect("Failed to attach the Table");

        // Return back after wrapping in Box
        session_context_ptr = Box::into_raw(Box::new(ctx)) as jlong
    });

    session_context_ptr
}


#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_next(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };

    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        //let fetch_start = std::time::Instant::now();
        let next = stream.try_next().await;
        //let fetch_time = fetch_start.elapsed();
        match next {
            Ok(Some(batch)) => {
                //let convert_start = std::time::Instant::now();
                // Convert to struct array for compatibility with FFI
                //println!("Num rows : {}", batch.num_rows());
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                //let convert_time = convert_start.elapsed();
                // ffi_array must remain alive until after the callback is called
                // let callback_start = std::time::Instant::now();
                set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_array));
                // let callback_time = callback_start.elapsed();
                // println!("Fetch: {:?}, Convert: {:?}, Callback: {:?}",
                //          fetch_time, convert_time, callback_time);
            }
            Ok(None) => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
        //println!("Total time: {:?}", start.elapsed());
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_getSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong,
    callback: JObject,
) {
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema);
    match ffi_schema {
        Ok(mut ffi_schema) => {
            // ffi_schema must remain alive until after the callback is called
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &err);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_closeStream(
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong
) {
    let _ = unsafe { Box::from_raw(stream as *mut SendableRecordBatchStream) };
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
    runtime: jlong
) {
    let _ = unsafe { Box::from_raw(runtime as *mut RuntimeEnv) };
}

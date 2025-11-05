/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use std::sync::Arc;
use std::time::Instant;
// std imports
use std::ptr::{addr_of_mut, null_mut}; // Needed for FFI helpers

// jni imports
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};

// arrow imports
use arrow_array::{Array, BinaryArray, StructArray};
use arrow_schema::ArrowError;

// Local modules
mod util;
mod row_id_optimizer;
mod listing_table;

// ++ DataFusion Imports ++
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::col as phys_col;
use datafusion_functions_aggregate::approx_distinct::ApproxDistinct;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;

use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::util::{create_object_meta_from_filenames, parse_string_arr, set_object_result_error, set_object_result_ok};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr}; // Import the builder AND the struct it builds
use datafusion_expr::AggregateUDF;

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

use futures::TryStreamExt;
use object_store::ObjectMeta;
use tokio::runtime::Runtime;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

// --- JNI Functions (Placeholders + ShardView ---
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
    _substrait_bytes: jbyteArray,
    tokio_runtime_env_ptr: jlong,
) -> jlong {

    // Wrap the main logic in a panic-safe block
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if shard_view_ptr == 0 || tokio_runtime_env_ptr == 0 {
            let _ = env.throw_new("java/lang/RuntimeException", "Rust received null pointer for ShardView or Tokio Runtime");
            return 0; // <-- CHANGE 2: Return 0 on error
        }

        let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
        let runtime = unsafe { &*(tokio_runtime_env_ptr as *const Runtime) };

        let table_path = shard_view.table_path();
        let files_meta = shard_view.files_meta();

        println!("[RUST] executeSubstraitQuery (Hardcoded Partial Agg -> Stream Ptr)");
        // ... (Setup code for table_path, files_meta, list_file_cache remains the same) ...
        // ... (RuntimeEnvBuilder code remains the same) ...
        // ... (SessionConfig and SessionStateBuilder remain the same) ...

        // --- REPEAT INITIAL SETUP FOR CONTEXT (Abbreviated for brevity in diff, keep your actual code here) ---
        let list_file_cache = Arc::new(DefaultListFilesCache::default());
        list_file_cache.put(table_path.prefix(), files_meta);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_cache_manager(CacheManagerConfig::default().with_list_files_cache(Some(list_file_cache.clone())))
            .build().unwrap(); // Simplified unwrap for brevity here, keep your error handling if you prefer
        let config = SessionConfig::new();
        let state = datafusion::execution::SessionStateBuilder::new().with_config(config).with_runtime_env(Arc::new(runtime_env)).with_default_features().build();
        let ctx = SessionContext::new_with_state(state);
        // -----------------------------------------------------------------------------------------

        // ... (Table registration code remains the same) ...
        // ** DEBUG: Manually register table here for testing **
        let register_result: core::result::Result<(), String> = runtime.block_on(async {
            // ... (keep your exact existing table registration logic here) ...
            let file_format = ParquetFormat::new();
            let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");
            let resolved_schema = listing_options.infer_schema(&ctx.state(), &table_path).await.map_err(|e| e.to_string())?;
            let config = ListingTableConfig::new(table_path).with_listing_options(listing_options).with_schema(resolved_schema);
            let provider = Arc::new(ListingTable::try_new(config).map_err(|e| e.to_string())?);
            ctx.register_table("index-7", provider).map_err(|e| e.to_string())?;
            Ok(())
        });
        if let Err(e) = register_result {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            return 0;
        }

        // --- Main Execution Logic ---
        // CHANGE 3: Return type of block_on is now Result<jlong, DataFusionError>
        let stream_ptr_result: Result<jlong, DataFusionError> = runtime.block_on(async {
            println!("[RUST] 1. Looking up table 'index-7'...");
            let df = ctx.table("index-7").await?;

            // ... (Keep steps 2 through 8 exactly the same to build 'final_plan') ...
            // [RUST] 2. Creating physical plan...
            let scan_phys_plan = ctx.state().create_physical_plan(df.logical_plan()).await?;
            let scan_schema = scan_phys_plan.schema();
            // [RUST] 3. Finding index for 'message'...
            let message_index = scan_schema.index_of("message")?;
            // [RUST] 4. Creating ProjectionExec...
            let projection_exprs = vec![(Arc::new(PhysicalColumn::new("message", message_index)) as Arc<dyn PhysicalExpr>, "message".to_string())];
            let projection_exec = Arc::new(ProjectionExec::try_new(projection_exprs, scan_phys_plan)?);
            let projected_schema = projection_exec.schema();
            // [RUST] 5. Creating aggregate UDF...
            let udaf_impl = ApproxDistinct::new();
            let agg_udf = Arc::new(AggregateUDF::new_from_impl(udaf_impl));
            let args_phys = vec![phys_col("message", &projected_schema)?];
            // [RUST] 6. Building AggregateExpr...
            let agg_expr_struct = AggregateExprBuilder::new(agg_udf, args_phys).schema(projected_schema.clone()).alias("hll_sketch_internal".to_string()).build()?;
            let agg_expr_arc = Arc::new(agg_expr_struct);
            // [RUST] 7. Creating Partial AggregateExec...
            let partial_agg_exec = Arc::new(AggregateExec::try_new(AggregateMode::Partial, PhysicalGroupBy::default(), vec![agg_expr_arc], vec![None], projection_exec, projected_schema.clone())?) as Arc<dyn ExecutionPlan>;
            // [RUST] 8. Creating final ProjectionExec for renaming...
            let final_schema = partial_agg_exec.schema();
            let intermediate_name = final_schema.field(0).name();
            let rename_exprs = vec![(phys_col(intermediate_name, &final_schema)?, "hll_sketch".to_string())];
            let final_plan = Arc::new(ProjectionExec::try_new(rename_exprs, partial_agg_exec)?) as Arc<dyn ExecutionPlan>;


            // --- CHANGE 4: Execute stream, but DO NOT collect. Return pointer. ---
            println!("[RUST] 9. Executing physical plan to get stream...");
            let task_ctx = ctx.task_ctx();
            // execute() returns Result<SendableRecordBatchStream>
            let stream = final_plan.execute(0, task_ctx)?;

            println!("[RUST] 10. Boxing stream and returning pointer.");
            // Box the stream and convert to raw pointer for Java
            let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;

            Ok(stream_ptr)
        });

        match stream_ptr_result {
            Ok(ptr) => {
                println!("[RUST] Success. Returning stream pointer: {}", ptr);
                ptr
            }
            Err(e) => {
                println!("[RUST] Error during plan setup: {}", e);
                let _ = env.throw_new("java/lang/RuntimeException", format!("Failed to create stream: {}", e));
                0
            }
        }
    }));

    match result {
        Ok(ptr) => ptr,
        Err(_) => {
            println!("[RUST] PANIC caught!");
            let _ = env.throw_new("java/lang/RuntimeException", "Rust native code panicked!");
            0
        }
    }
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
// std imports
use std::sync::Arc;
use std::ptr::{addr_of_mut, null_mut}; // Needed for FFI helpers

// jni imports
use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;

// arrow imports
use arrow_array::{Array, BinaryArray, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
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

// CORRECTED Imports based on new understanding
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr}; // Import the builder AND the struct it builds
// Import AggregateUDF (logical definition)
use datafusion_expr::AggregateUDF;

use datafusion::execution::context::SessionContext;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingTableUrl};
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::DefaultListFilesCache;
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionConfig;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::DATAFUSION_VERSION;

// Other necessary imports
use futures::TryStreamExt;
use object_store::ObjectMeta;
use tokio::runtime::Runtime;

// Local crate imports
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::util::{create_object_meta_from_filenames, parse_string_arr, set_object_result_error, set_object_result_ok};
// -- End DataFusion Imports --


// --- JNI Functions (Placeholders + ShardView ---
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createContext(/*...*/) -> jlong { /* Placeholder */ Box::into_raw(Box::new(SessionContext::new())) as jlong }
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeContext(_env: JNIEnv, _class: JClass, ptr: jlong) { if ptr != 0 { let _ = unsafe { Box::from_raw(ptr as *mut SessionContext) }; } }
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersionInfo(env: JNIEnv, _class: JClass) -> jstring { let v = format!(r#"{{"version": "{}", "codecs": ["PlaceholderCodec"]}}"#, DATAFUSION_VERSION); env.new_string(v).unwrap().into_raw() }
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersion(env: JNIEnv, _class: JClass) -> jstring { env.new_string(DATAFUSION_VERSION).unwrap().into_raw() }
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createTokioRuntime(/*...*/) -> jlong { Box::into_raw(Box::new(Runtime::new().unwrap())) as jlong }
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createGlobalRuntime(/*...*/) -> jlong { Box::into_raw(Box::new(RuntimeEnvBuilder::default().build().unwrap())) as jlong }
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createSessionContext(_env: JNIEnv, _class: JClass, runtime_ptr: jlong) -> jlong { if runtime_ptr == 0 { return 0; } let rt_arc = unsafe { Arc::from_raw(runtime_ptr as *const RuntimeEnv) }; let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), rt_arc.clone()); std::mem::forget(rt_arc); Box::into_raw(Box::new(ctx)) as jlong }
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeSessionContext(_env: JNIEnv, _class: JClass, ptr: jlong) { if ptr != 0 { let _ = unsafe { Box::from_raw(ptr as *mut SessionContext) }; } }
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createDatafusionReader(mut env: JNIEnv, _class: JClass, path: JString, files: JObjectArray) -> jlong {
    println!("[RUST] Creating Datafusion Reader (ShardView)");
    let table_path_str: String = match env.get_string(&path) { Ok(s) => s.into(), Err(e) => { eprintln!("Error getting table path string: {}", e); return 0; } };
    let files_vec: Vec<String> = match parse_string_arr(&mut env, files) { Ok(v) => v, Err(e) => { eprintln!("Error parsing file list: {}", e); return 0; } };
    let files_meta = create_object_meta_from_filenames(&table_path_str, files_vec);
    match ListingTableUrl::parse(&table_path_str) { Ok(url) => Box::into_raw(Box::new(ShardView::new(url, files_meta))) as jlong, Err(e) => { eprintln!("Error parsing ListingTableUrl: {}", e); 0 } }
}
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_destroyReader(_env: JNIEnv, _class: JClass, ptr: jlong) { if ptr != 0 { let _ = unsafe { Box::from_raw(ptr as *mut ShardView) }; } }

// --- ShardView struct and impl ---
pub struct ShardView { table_path: ListingTableUrl, files_meta: Arc<Vec<ObjectMeta>> }
impl ShardView { pub fn new(table_path: ListingTableUrl, files_meta: Vec<ObjectMeta>) -> Self { ShardView { table_path, files_meta: Arc::new(files_meta) } } pub fn table_path(&self) -> ListingTableUrl { self.table_path.clone() } pub fn files_meta(&self) -> Arc<Vec<ObjectMeta>> { self.files_meta.clone() } }


// --- executeSubstraitQuery (Hardcoded Version - Final Fixes based on User Feedback) ---
// --- executeSubstraitQuery (Hardcoded Version - More Logging) ---
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_executeSubstraitQuery(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    _substrait_bytes: jbyteArray,
    tokio_runtime_env_ptr: jlong,
) -> jbyteArray {
    // Helper function to create an empty Java byte array
    fn empty_byte_array(env: &mut JNIEnv) -> jbyteArray {
        env.new_byte_array(0)
            .unwrap_or_else(|_| env.new_byte_array(0).unwrap())
            .into_raw()
    }

    // Wrap the main logic in a panic-safe block
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if shard_view_ptr == 0 || tokio_runtime_env_ptr == 0 {
            let _ = env.throw_new("java/lang/RuntimeException", "Rust received null pointer for ShardView or Tokio Runtime");
            return empty_byte_array(&mut env);
        }

        let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
        let runtime = unsafe { &*(tokio_runtime_env_ptr as *const Runtime) };

        let table_path = shard_view.table_path();
        let files_meta = shard_view.files_meta();

        println!("[RUST] executeSubstraitQuery (Hardcoded Partial Agg)");
        println!("[RUST] Table path: {}", table_path);
        println!("[RUST] Files: {:?}", files_meta);

        // --- Context & Table Setup ---
        let list_file_cache = Arc::new(DefaultListFilesCache::default());
        list_file_cache.put(table_path.prefix(), files_meta);

        let runtime_env = match RuntimeEnvBuilder::new()
            .with_cache_manager(CacheManagerConfig::default().with_list_files_cache(Some(list_file_cache.clone())))
            .build()
        {
            Ok(re) => re,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to build RuntimeEnv: {}", e),
                );
                return empty_byte_array(&mut env);
            }
        };

        let config = SessionConfig::new();
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .build();

        let ctx = SessionContext::new_with_state(state);

        // Placeholder for table registration
        // ** DEBUG: Manually register table here for testing **
        let register_result: core::result::Result<(), String> =
            runtime.block_on(async {
                let file_format = ParquetFormat::new();
                let listing_options = ListingOptions::new(Arc::new(file_format))
                    .with_file_extension(".parquet");

                let resolved_schema = match listing_options.infer_schema(&ctx.state(), &table_path).await {
                    Ok(s) => s,
                    Err(e) => return Err(format!("Schema infer failed: {}", e)),
                };

                let config = ListingTableConfig::new(table_path)
                    .with_listing_options(listing_options)
                    .with_schema(resolved_schema);

                let provider = match ListingTable::try_new(config) {
                    Ok(p) => Arc::new(p),
                    Err(e) => return Err(format!("ListingTable create failed: {}", e)),
                };

                match ctx.register_table("index-7", provider) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(format!("Register table failed: {}", e)),
                }
            });

        if let Err(e) = register_result {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            return empty_byte_array(&mut env);
        }

        println!("[RUST] Table 'index-7' registered successfully.");

        // --- Main Execution Logic ---
        let hll_bytes_result: Result<Vec<u8>, DataFusionError> = runtime.block_on(async {

            println!("[RUST] 1. Looking up table 'index-7'...");
            let df = ctx.table("index-7").await?;
            println!("[RUST] 2. Creating physical plan...");
            let scan_phys_plan = ctx.state().create_physical_plan(df.logical_plan()).await?;
            let scan_schema = scan_phys_plan.schema();

            println!("[RUST] 3. Finding index for 'message'...");
            let message_index = scan_schema.index_of("message")?;
            println!("[RUST] 4. Creating ProjectionExec...");
            let projection_exprs = vec![(
                Arc::new(PhysicalColumn::new("message", message_index)) as Arc<dyn PhysicalExpr>,
                "message".to_string(),
            )];
            let projection_exec = Arc::new(ProjectionExec::try_new(projection_exprs, scan_phys_plan)?);
            let projected_schema: ArrowSchemaRef = projection_exec.schema();

            println!("[RUST] 5. Creating aggregate UDF...");
            let udaf_impl = ApproxDistinct::new();
            let agg_udf = Arc::new(AggregateUDF::new_from_impl(udaf_impl));
            let args_phys = vec![phys_col("message", &projected_schema)?];

            println!("[RUST] 6. Building AggregateExpr...");
            //
            // --- THIS IS THE FIX ---
            //
            let agg_expr_struct: AggregateFunctionExpr =
                AggregateExprBuilder::new(agg_udf, args_phys)
                    .schema(projected_schema.clone()) // Add the input schema
                    .alias("hll_sketch_internal".to_string()) // <-- FIX: Use .alias()
                    .build()?;
            // --- END FIX ---

            let agg_expr_arc = Arc::new(agg_expr_struct);

            println!("[RUST] 7. Creating Partial AggregateExec...");
            let partial_agg_exec = Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::default(),
                vec![agg_expr_arc],
                vec![None],
                projection_exec,
                projected_schema.clone(),
            )?) as Arc<dyn ExecutionPlan>;

            println!("[RUST] 8. Creating final ProjectionExec for renaming...");
            let final_schema = partial_agg_exec.schema();
            let intermediate_name = final_schema.field(0).name();
            let rename_exprs = vec![(phys_col(intermediate_name, &final_schema)?, "hll_sketch".to_string())];
            let final_plan = Arc::new(ProjectionExec::try_new(rename_exprs, partial_agg_exec)?) as Arc<dyn ExecutionPlan>;

            println!("[RUST] 9. Executing physical plan...");
            let task_ctx = ctx.task_ctx();
            let mut stream = final_plan.execute(0, task_ctx)?; // Partition 0

            println!("[RUST] 10. Collecting results from stream...");
            let batches = datafusion::physical_plan::common::collect(stream).await?;
            println!("[RUST] 11. Collected {} batches.", batches.len());

            // Extract HLL sketch bytes
            if let Some(batch) = batches.first() {
                if let Some(col) = batch.column_by_name("hll_sketch") {
                    if let Some(binary_array) = col.as_any().downcast_ref::<BinaryArray>() {
                        if binary_array.len() == 1 && !binary_array.is_null(0) {
                            println!("[RUST] 12. Successfully found sketch bytes.");
                            return Ok(binary_array.value(0).to_vec());
                        }
                    }
                }
            }
            println!("[RUST] 12. Failed to extract sketch from result batch.");
            Err(DataFusionError::Execution("Failed to extract HLL sketch".into()))
        });

        // Convert to JNI byte array
        match hll_bytes_result {
            Ok(bytes) => {
                println!("[RUST] Success. Returning {} bytes.", bytes.len());
                env.byte_array_from_slice(&bytes)
                    .map(|arr| arr.into_raw())
                    .unwrap_or_else(|_| empty_byte_array(&mut env))
            }
            Err(e) => {
                println!("[RUST] Error during execution: {}", e);
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to generate HLL sketch: {}", e),
                );
                empty_byte_array(&mut env)
            }
        }
    }));

    // Handle outer panic
    match result {
        Ok(j_byte_array_ptr) => j_byte_array_ptr,
        Err(_) => {
            println!("[RUST] PANIC caught by catch_unwind!");
            let _ = env.throw_new("java/lang/RuntimeException", "Rust native code panicked!");
            empty_byte_array(&mut env)
        }
    }
}


// --- Corrected RecordBatchStream JNI functions (Restored Logic) ---
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_next(mut env: JNIEnv, _class: JClass, runtime_ptr: jlong, stream_ptr: jlong, callback: JObject,) {
    if runtime_ptr == 0 || stream_ptr == 0 || callback.is_null() { return; }
    let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    let stream = unsafe { &mut *(stream_ptr as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        match stream.try_next().await {
            Ok(Some(batch)) => {
                // Use StructArray to ensure FFI compatibility if schema has multiple columns potentially
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_array));
            }
            Ok(None) => {
                set_object_result_ok::<FFI_ArrowArray>(&mut env, callback, null_mut());
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
    });
}
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_RecordBatchStream_getSchema(mut env: JNIEnv, _class: JClass, stream_ptr: jlong, callback: JObject,) {
    if stream_ptr == 0 || callback.is_null() { return; }
    let stream = unsafe { &mut *(stream_ptr as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    match FFI_ArrowSchema::try_from(schema.as_ref()) {
        Ok(mut ffi_schema) => {
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            let df_err = DataFusionError::ArrowError(Box::new(err), None);
            set_object_result_error(&mut env, callback, &df_err);
        }
    }
}

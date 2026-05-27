/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Generic `state → scalar` finalize ScalarUDF wrapper.
//!
//! Decodes IPC-encoded Binary state and returns `inner.evaluate()`. Used as the
//! per-shard sort-key expression (`AggregateFunction.<X>.finalizeOperator`) by
//! `OpenSearchAggregateShardBucketRule`. Per-aggregate cost: one line of registration.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    AggregateUDF, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::PhysicalExpr;

/// Builds a `ScalarUDF` named `name` that decodes IPC-encoded state for
/// `inner` and returns `inner.evaluate(state)`.
///
/// `inner_native_input_type` is the post-coercion input type the inner UDAF
/// would see in PARTIAL mode. It's used to resolve the inner's return type
/// and state schema during construction (the inner's `state_fields` typically
/// depends on input type, e.g. `count(distinct)` returns `List<input_type>`).
pub fn state_finalize_udf(
    inner: Arc<AggregateUDF>,
    name: &str,
    inner_native_input_type: DataType,
) -> Result<ScalarUDF> {
    let return_type = inner.return_type(&[inner_native_input_type.clone()])?;
    let state_schema = compute_state_schema(&inner, name, &inner_native_input_type)?;
    Ok(ScalarUDF::from(StateFinalizeUdf {
        inner,
        name: name.to_string(),
        return_type,
        state_schema,
        inner_native_input_type,
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Binary]),
                TypeSignature::Exact(vec![DataType::LargeBinary]),
            ],
            Volatility::Immutable,
        ),
    }))
}

/// Resolves the inner UDAF's IPC state schema by calling `inner.state_fields`
/// with a synthetic `StateFieldsArgs` matching what `StateShippingUdaf` uses.
fn compute_state_schema(
    inner: &Arc<AggregateUDF>,
    name: &str,
    native_arg_type: &DataType,
) -> Result<SchemaRef> {
    let inner_return_type = inner.return_type(&[native_arg_type.clone()])?;
    let return_field = Arc::new(Field::new("__inner_return", inner_return_type, true));
    let input_field: FieldRef = Arc::new(Field::new("__native", native_arg_type.clone(), true));
    let input_fields = [input_field];
    let args = StateFieldsArgs {
        name,
        input_fields: &input_fields,
        return_field,
        ordering_fields: &[],
        is_distinct: false,
    };
    let inner_state_fields: Vec<Field> = inner
        .state_fields(args)?
        .into_iter()
        .map(|f| (*f).clone())
        .collect();
    Ok(Arc::new(Schema::new(inner_state_fields)))
}

/// Generic finalize wrapper. `(state: Binary) → inner.return_type`.
pub struct StateFinalizeUdf {
    inner: Arc<AggregateUDF>,
    name: String,
    return_type: DataType,
    state_schema: SchemaRef,
    inner_native_input_type: DataType,
    signature: Signature,
}

impl Debug for StateFinalizeUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StateFinalizeUdf(name={}, inner={}, return_type={:?})",
            self.name,
            self.inner.name(),
            self.return_type
        )
    }
}

impl PartialEq for StateFinalizeUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.inner.name() == other.inner.name()
    }
}

impl Eq for StateFinalizeUdf {}

impl Hash for StateFinalizeUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.inner.name().hash(state);
    }
}

impl ScalarUDFImpl for StateFinalizeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!(
                "{}: expected exactly 1 argument, got {}",
                self.name,
                args.args.len()
            );
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(opt) | ScalarValue::LargeBinary(opt)) => {
                let scalar = match opt.as_ref() {
                    None => null_scalar_for(&self.return_type)?,
                    Some(bytes) => self.finalize_one(bytes)?,
                };
                Ok(ColumnarValue::Scalar(scalar))
            }
            ColumnarValue::Scalar(other) => {
                exec_err!("{}: expected Binary input, got {other:?}", self.name)
            }
            ColumnarValue::Array(arr) => self.finalize_array(arr.as_ref()),
        }
    }
}

impl StateFinalizeUdf {
    /// Finalize a single Binary IPC blob → user-facing scalar.
    fn finalize_one(&self, bytes: &[u8]) -> Result<ScalarValue> {
        let arrays = decode_state_arrays(bytes, &self.state_schema)?;
        let mut acc = self.build_inner_accumulator()?;
        acc.merge_batch(&arrays)?;
        acc.evaluate()
    }

    /// Finalize a Binary array column. Each row is an independent IPC blob;
    /// each gets its own fresh accumulator (no cross-row merge — the wire
    /// already contains a per-group merged state).
    fn finalize_array(&self, arr: &dyn Array) -> Result<ColumnarValue> {
        let bin = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{}: expected BinaryArray, got {:?}",
                self.name,
                arr.data_type()
            ))
        })?;
        let mut scalars: Vec<ScalarValue> = Vec::with_capacity(bin.len());
        for i in 0..bin.len() {
            if bin.is_null(i) {
                scalars.push(null_scalar_for(&self.return_type)?);
            } else {
                scalars.push(self.finalize_one(bin.value(i))?);
            }
        }
        let out_array = ScalarValue::iter_to_array(scalars)?;
        Ok(ColumnarValue::Array(out_array))
    }

    /// Build a fresh inner accumulator with a synthetic `AccumulatorArgs`.
    /// Matches the synthetic args `StateShippingUdaf::build_inner_accumulator`
    /// uses, so the inner sees the same shape as on the data-node side.
    fn build_inner_accumulator(&self) -> Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        let native_field: FieldRef = Arc::new(Field::new(
            "__native",
            self.inner_native_input_type.clone(),
            true,
        ));
        let native_schema = Arc::new(Schema::new(vec![(*native_field).clone()]));
        let native_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("__native", 0));
        let native_exprs = [native_expr];
        let native_field_refs = [native_field.clone()];
        let inner_return_field: FieldRef =
            Arc::new(Field::new("__inner_return", self.return_type.clone(), true));
        let inner_args = AccumulatorArgs {
            return_field: inner_return_field,
            schema: &native_schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: &self.name,
            is_distinct: false,
            exprs: &native_exprs,
            expr_fields: &native_field_refs,
        };
        self.inner.accumulator(inner_args)
    }
}

/// Decode IPC bytes produced by `StateShippingAccumulator::state()` into
/// the per-column `ArrayRef` slices `Accumulator::merge_batch` expects.
fn decode_state_arrays(bytes: &[u8], schema: &SchemaRef) -> Result<Vec<ArrayRef>> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let batch: RecordBatch = reader.next().ok_or_else(|| {
        DataFusionError::Execution(
            "state-finalize: empty IPC stream while decoding state".to_string(),
        )
    })??;
    if batch.schema().fields().len() != schema.fields().len() {
        return exec_err!(
            "state-finalize decode: schema mismatch — expected {} fields, got {}",
            schema.fields().len(),
            batch.schema().fields().len()
        );
    }
    Ok(batch.columns().to_vec())
}

fn null_scalar_for(dt: &DataType) -> Result<ScalarValue> {
    ScalarValue::try_from(dt)
}

/// Registers the canonical set of state-finalize ScalarUDFs.
///
/// One per state-shipping aggregate. Each name matches the corresponding
/// `AggregateFunction.<X>.finalizeOperator` declared on the Java SPI side.
///
/// Construction is infallible in practice — the inner UDAFs are built-in
/// DataFusion functions whose `return_type` and `state_fields` accept the
/// canonical input types we pass. Any error here would indicate a DataFusion
/// API regression and is surfaced as a panic at startup.
pub fn register_all(ctx: &SessionContext) {
    use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
    use datafusion::functions_aggregate::average::avg_udaf;
    use datafusion::functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
    use datafusion::functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};

    let registrations: &[(Arc<AggregateUDF>, &'static str, DataType)] = &[
        // Approximate distinct count: Binary HLL state → Int64 cardinality.
        // Replaces the bespoke `udf::hll_estimate`.
        (approx_distinct_udaf(), "hll_estimate", DataType::Int64),
        // AVG/STDDEV/VAR finalizers used by `OpenSearchAggregateShardBucketRule`'s
        // per-shard sort-key expression.
        (avg_udaf(), "avg_finalize", DataType::Float64),
        (stddev_pop_udaf(), "stddev_pop_finalize", DataType::Float64),
        (stddev_udaf(), "stddev_samp_finalize", DataType::Float64),
        (var_pop_udaf(), "var_pop_finalize", DataType::Float64),
        (var_samp_udaf(), "var_samp_finalize", DataType::Float64),
    ];
    for (inner, name, native_input) in registrations {
        let udf = state_finalize_udf(Arc::clone(inner), name, native_input.clone())
            .unwrap_or_else(|e| panic!("state_finalize_udf({name}) construction failed: {e}"));
        ctx.register_udf(udf);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Generic state-shipping wrapper for DataFusion's native aggregate UDAFs.
//!
//! Collapses multi-column accumulator state into a single Binary column (Arrow IPC)
//! for substrait wire transport. Reverses the encoding on FINAL-side merge. Delegates
//! all algorithm work to the inner native UDAF. Per-aggregate cost: one line of registration.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;

/// Registers state-shipping wrappers for all distributed aggregates.
pub fn register_all(ctx: &SessionContext) {
    use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
    use datafusion::functions_aggregate::average::avg_udaf;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
    use datafusion::functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};

    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(sum_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(min_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(max_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(count_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(avg_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(approx_distinct_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(stddev_pop_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(stddev_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::with_name(stddev_udaf(), "stddev_samp".to_string())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(var_pop_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::new(var_samp_udaf())));
    ctx.register_udaf(AggregateUDF::from(StateShippingUdaf::with_name(var_samp_udaf(), "var_samp".to_string())));
}

/// True when substrait declares INITIAL_TO_INTERMEDIATE phase on any aggregate measure.
///
/// Used to gate `Mode::Partial` stripping: only strip when both the wrapper UDAF
pub fn substrait_has_partial_phase(plan: &substrait::proto::Plan) -> bool {
    use substrait::proto::plan_rel::RelType as PlanRelType;
    let phase_partial = substrait::proto::AggregationPhase::InitialToIntermediate as i32;
    fn rel_has_phase(rel: &substrait::proto::Rel, target: i32) -> bool {
        use substrait::proto::rel::RelType;
        let Some(rt) = rel.rel_type.as_ref() else { return false };
        match rt {
            RelType::Aggregate(agg) => {
                for m in &agg.measures {
                    if let Some(measure) = m.measure.as_ref() {
                        if measure.phase == target {
                            return true;
                        }
                    }
                }
                agg.input.as_deref().map(|r| rel_has_phase(r, target)).unwrap_or(false)
            }
            RelType::Project(p) => p.input.as_deref().map(|r| rel_has_phase(r, target)).unwrap_or(false),
            RelType::Filter(f) => f.input.as_deref().map(|r| rel_has_phase(r, target)).unwrap_or(false),
            RelType::Sort(s) => s.input.as_deref().map(|r| rel_has_phase(r, target)).unwrap_or(false),
            RelType::Fetch(f) => f.input.as_deref().map(|r| rel_has_phase(r, target)).unwrap_or(false),
            RelType::Join(j) => {
                j.left.as_deref().map(|r| rel_has_phase(r, target)).unwrap_or(false)
                    || j.right.as_deref().map(|r| rel_has_phase(r, target)).unwrap_or(false)
            }
            RelType::Set(s) => s.inputs.iter().any(|r| rel_has_phase(r, target)),
            _ => false,
        }
    }
    for plan_rel in &plan.relations {
        let Some(rt) = plan_rel.rel_type.as_ref() else { continue };
        let rel: &substrait::proto::Rel = match rt {
            PlanRelType::Rel(r) => r,
            PlanRelType::Root(root) => match root.input.as_ref() {
                Some(r) => r,
                None => continue,
            },
        };
        if rel_has_phase(rel, phase_partial) {
            return true;
        }
    }
    false
}

/// Wraps an inner `AggregateUDF` to ship state as a single `Binary` column.
pub struct StateShippingUdaf {
    inner: Arc<AggregateUDF>,
    name: String,
    signature: Signature,
}

impl StateShippingUdaf {
    pub fn new(inner: Arc<AggregateUDF>) -> Self {
        let name = inner.name().to_string();
        Self::with_name(inner, name)
    }

    /// Build wrapper under an alias name for substrait resolution.
    pub fn with_name(inner: Arc<AggregateUDF>, name: String) -> Self {
        // `user_defined` defers all type checking to our `coerce_types`; the
        // analyzer never tries to match against the signature directly.
        let signature = Signature::user_defined(Volatility::Immutable);
        Self { inner, name, signature }
    }

    fn is_state_input(arg_type: &DataType) -> bool {
        matches!(arg_type, DataType::Binary | DataType::LargeBinary)
    }

    /// Arrow schema for the inner UDAF's state shape (IPC encode/decode target).
    fn inner_state_schema(&self, native_arg_type: &DataType, is_distinct: bool) -> Result<SchemaRef> {
        let inner_return_type = self.inner.return_type(&[native_arg_type.clone()])?;
        let return_field = Arc::new(Field::new("__inner_return", inner_return_type, true));
        let input_field: FieldRef = Arc::new(Field::new("__native", native_arg_type.clone(), true));
        let input_fields = [input_field];
        let args = StateFieldsArgs {
            name: &self.name,
            input_fields: &input_fields,
            return_field,
            ordering_fields: &[],
            is_distinct,
        };
        let inner_state_fields: Vec<Field> = self
            .inner
            .state_fields(args)?
            .into_iter()
            .map(|f| (*f).clone())
            .collect();
        Ok(Arc::new(Schema::new(inner_state_fields)))
    }
}

impl Debug for StateShippingUdaf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StateShippingUdaf({})", self.name)
    }
}

impl PartialEq for StateShippingUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for StateShippingUdaf {}
impl Hash for StateShippingUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl AggregateUDFImpl for StateShippingUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Coerces input types: Binary→pass-through (FINAL), numeric→Float64, else delegate.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return exec_err!("{}: missing argument", self.name);
        }
        let arg = &arg_types[0];
        if Self::is_state_input(arg) {
            return Ok(vec![arg.clone()]);
        }
        if arg.is_numeric() {
            return Ok(vec![DataType::Float64]);
        }
        match self.inner.coerce_types(arg_types) {
            Ok(coerced) => Ok(coerced),
            Err(_) => Ok(arg_types.to_vec()),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("{}: missing argument", self.name);
        }
        // Probe with Float64; return shape is input-independent for wrapped UDAFs.
        let probe_type = if Self::is_state_input(&arg_types[0]) {
            DataType::Float64
        } else if arg_types[0].is_numeric() {
            DataType::Float64
        } else {
            arg_types[0].clone()
        };
        self.inner.return_type(&[probe_type])
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Use args.name so column name matches substrait's ReadRel declaration.
        Ok(vec![Arc::new(Field::new(
            args.name.to_string(),
            DataType::Binary,
            true,
        ))])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let arg_type = acc_args
            .exprs
            .first()
            .map(|e| e.data_type(acc_args.schema))
            .transpose()?
            .ok_or_else(|| {
                DataFusionError::Execution(format!("{}: missing first argument", self.name))
            })?;
        let is_final = Self::is_state_input(&arg_type);

        let native_arg_type = if is_final {
            DataType::Float64
        } else {
            arg_type
        };

        let inner_acc = self.build_inner_accumulator(&native_arg_type, acc_args.is_distinct)?;
        let state_schema = self.inner_state_schema(&native_arg_type, acc_args.is_distinct)?;

        Ok(Box::new(StateShippingAccumulator {
            inner: inner_acc,
            state_schema,
            is_final,
        }))
    }
}

impl StateShippingUdaf {
    /// Constructs the inner accumulator with synthetic AccumulatorArgs.
    fn build_inner_accumulator(&self, native_arg_type: &DataType, is_distinct: bool) -> Result<Box<dyn Accumulator>> {
        let native_field: FieldRef =
            Arc::new(Field::new("__native", native_arg_type.clone(), true));
        let native_schema = Arc::new(Schema::new(vec![(*native_field).clone()]));
        let native_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("__native", 0));
        let native_exprs = [native_expr];
        let native_field_refs = [native_field.clone()];
        let inner_return_type = self.inner.return_type(&[native_arg_type.clone()])?;
        let inner_return_field: FieldRef =
            Arc::new(Field::new("__inner_return", inner_return_type, true));
        let inner_args = AccumulatorArgs {
            return_field: inner_return_field,
            schema: &native_schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: &self.name,
            is_distinct,
            exprs: &native_exprs,
            expr_fields: &native_field_refs,
        };
        self.inner.accumulator(inner_args)
    }
}

/// Accumulator that IPC-encodes/decodes between Binary wire format and inner state.
struct StateShippingAccumulator {
    inner: Box<dyn Accumulator>,
    state_schema: SchemaRef,
    is_final: bool,
}

impl Debug for StateShippingAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StateShippingAccumulator(is_final={}, state_schema={:?})",
            self.is_final, self.state_schema
        )
    }
}

impl StateShippingAccumulator {
    /// Decodes BinaryArray of IPC blobs and drives inner.merge_batch.
    fn merge_state_binary(&mut self, binary_array: &ArrayRef) -> Result<()> {
        let arr = binary_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "StateShippingAccumulator: expected BinaryArray for state input".to_string(),
                )
            })?;
        let num_state_cols = self.state_schema.fields().len();
        // Row-major decode → column-major rebuild. Skip null rows.
        let mut col_values: Vec<Vec<ScalarValue>> =
            vec![Vec::with_capacity(arr.len()); num_state_cols];
        for row_idx in 0..arr.len() {
            if arr.is_null(row_idx) {
                continue;
            }
            let bytes = arr.value(row_idx);
            let row_state = decode_state_from_ipc(bytes, &self.state_schema)?;
            if row_state.len() != num_state_cols {
                return exec_err!(
                    "state-shipping decode: expected {num_state_cols} state fields, got {}",
                    row_state.len()
                );
            }
            for (col_idx, sv) in row_state.into_iter().enumerate() {
                col_values[col_idx].push(sv);
            }
        }
        if col_values.first().map(|c| c.is_empty()).unwrap_or(true) {
            return Ok(());
        }
        let arrays: Vec<ArrayRef> = col_values
            .into_iter()
            .map(|col| ScalarValue::iter_to_array(col.into_iter()))
            .collect::<Result<_>>()?;
        self.inner.merge_batch(&arrays)
    }
}

impl Accumulator for StateShippingAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.is_final {
            self.merge_state_binary(&values[0])
        } else {
            self.inner.update_batch(values)
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.merge_state_binary(&states[0])
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let inner_state = self.inner.state()?;
        let bytes = encode_state_to_ipc(&inner_state, &self.state_schema)?;
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let result = self.inner.evaluate()?;
        Ok(result)
    }

    fn size(&self) -> usize {
        self.inner.size() + std::mem::size_of::<Self>()
    }
}

/// Encode accumulator state as Arrow IPC bytes.
fn encode_state_to_ipc(state: &[ScalarValue], schema: &SchemaRef) -> Result<Vec<u8>> {
    let arrays: Vec<ArrayRef> = state
        .iter()
        .map(|sv| sv.to_array_of_size(1))
        .collect::<Result<_>>()?;
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Decode Arrow IPC bytes into Vec<ScalarValue> state.
fn decode_state_from_ipc(bytes: &[u8], schema: &SchemaRef) -> Result<Vec<ScalarValue>> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let batch = reader
        .next()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "StateShippingAccumulator: empty IPC stream while decoding state".to_string(),
            )
        })??;
    if batch.schema().fields().len() != schema.fields().len() {
        return exec_err!(
            "state-shipping decode: schema mismatch — expected {} fields, got {}",
            schema.fields().len(),
            batch.schema().fields().len()
        );
    }
    let mut state = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let array = batch.column(col_idx);
        let scalar = ScalarValue::try_from_array(array, 0)?;
        state.push(scalar);
    }
    Ok(state)
}

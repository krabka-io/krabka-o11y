use super::{
    Arc, Array, ArrayRef, ColumnarValue, DataFusionError, DataType, DfResult, Float64Builder,
    Int64Array, RateFamily, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Time, TimeExt,
    Volatility, decode_range_column, scalar_i64,
};

/// A `ScalarUDFImpl` over `RangeManipulate`'s windowed columns.
///
/// There is one instance per [`RateFamily`] member, and the family selects the
/// math.
///
/// `ScalarUDFImpl` needs `Eq` and `Hash` through `DynEq` and `DynHash` so that
/// the planner can deduplicate and key on UDF identity. Both fields derive them.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct RateUdf {
    pub(crate) family: RateFamily,
    pub(crate) signature: Signature,
}

impl RateUdf {
    pub(crate) fn new(family: RateFamily) -> Self {
        Self {
            family,
            // Args mix Int64 scalars and Dictionary range columns, so type
            // coercion is bespoke: accept whatever the planner supplies and
            // validate shapes at invoke time.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RateUdf {
    fn name(&self) -> &str {
        self.family.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    /// `Signature::user_defined` needs its own type coercion.
    ///
    /// The rate UDFs accept their arguments unchanged. The `RangeArray`
    /// dictionary columns and the Int64 scalar are already the exact types that
    /// `RangeManipulate` makes, so no cast is wanted, and a cast of a
    /// `Dictionary<Int64, List<_>>` has no meaning. This method checks the arity
    /// and returns the types unchanged.
    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        if arg_types.len() != 4 {
            return Err(DataFusionError::Plan(format!(
                "{} expects 4 arguments (eval_timestamp, timestamp_range, value_range, range_ms), got {}",
                self.name(),
                arg_types.len()
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let name = self.name();
        if args.args.len() != 4 {
            return Err(DataFusionError::Execution(format!(
                "{name} expects 4 arguments (eval_timestamp, timestamp_range, value_range, range_ms), got {}",
                args.args.len()
            )));
        }
        let rows = args.number_rows;

        // 1. eval_timestamp column (Int64): range_end_ms per step.
        let eval_ts = args.args[0].clone().into_array(rows)?;
        let eval_ts = eval_ts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `eval_timestamp` must be Int64, got {:?}",
                    eval_ts.data_type()
                ))
            })?;

        // 2 & 3. The windowed timestamp and value RangeArrays.
        let timestamp_range = args.args[1].clone().into_array(rows)?;
        let timestamp_range = decode_range_column(&timestamp_range, "timestamp_range", name)?;
        let value_range = args.args[2].clone().into_array(rows)?;
        let value_range = decode_range_column(&value_range, "value_range", name)?;

        // 4. range_ms scalar (the range-selector width).
        let range = Time::from_millis(scalar_i64(&args.args[3], "range_ms", name)?);

        if timestamp_range.len() != rows || value_range.len() != rows || eval_ts.len() != rows {
            return Err(DataFusionError::Execution(format!(
                "{name}: row-count mismatch (eval_ts={}, timestamp_range={}, value_range={}, rows={rows})",
                eval_ts.len(),
                timestamp_range.len(),
                value_range.len()
            )));
        }

        let mut builder = Float64Builder::with_capacity(rows);
        for row in 0..rows {
            let timestamps = timestamp_range.timestamp_slice(row).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `timestamp_range` cell {row} is not Int64"
                ))
            })?;
            let values = value_range.value_slice(row).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `value_range` cell {row} is not Float64"
                ))
            })?;
            let eval = eval_ts.value(row);
            match self.family.eval_window(timestamps, values, eval, range) {
                // A genuinely-computed value (including a legitimately-NaN result)
                // is kept as a non-null float so it propagates through downstream
                // aggregates exactly as the interpreter propagates it.
                Some(value) => builder.append_value(value),
                // Prometheus has no value for this window (fewer than two samples,
                // zero-width interval). Emit NULL — not a NaN sentinel — so the
                // assembler drops the series and aggregates skip it, matching the
                // interpreter, which omits no-value series before aggregating.
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// The `rate` UDF: per-second, counter-reset-corrected, extrapolated rate.
#[must_use]
pub fn rate_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Rate))
}

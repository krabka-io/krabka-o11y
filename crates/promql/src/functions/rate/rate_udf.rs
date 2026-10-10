use super::{
    Arc, ArrayRef, ColumnarValue, DataFusionError, DataType, DfResult, Float64Builder, RateFamily,
    RateWindow, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Time, TimeExt, Volatility,
    WindowBounds, WindowColumns, scalar_i64,
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

        // 1-3. The eval_timestamp column (Int64, range_end_ms per step) and the
        // windowed timestamp and value RangeArrays.
        let windows = WindowColumns::decode(&args.args, rows, name)?;

        // 4. range_ms scalar (the range-selector width).
        let range = Time::from_millis(scalar_i64(&args.args[3], "range_ms", name)?);

        windows.check_rows(rows, name)?;

        let mut builder = Float64Builder::with_capacity(rows);
        for row in 0..rows {
            let (timestamps, values) = windows.window(row, name)?;
            let eval = windows.eval_ts.value(row);
            let window = RateWindow {
                timestamps,
                values,
                bounds: WindowBounds {
                    range_start_ms: eval - range.millis_i64(),
                    range_end_ms: eval,
                },
                range,
            };
            match self.family.eval_window(window) {
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

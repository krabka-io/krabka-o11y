use super::{
    Arc, ArrayRef, ColumnarValue, DataFusionError, DataType, DfResult, Float64Builder,
    OverTimeFamily, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    WindowColumns, scalar_f64,
};

/// A `ScalarUDFImpl` over `RangeManipulate`'s windowed columns.
///
/// There is one instance per [`OverTimeFamily`] member. The family selects the
/// reduction.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct OverTimeUdf {
    pub(crate) family: OverTimeFamily,
    pub(crate) signature: Signature,
}

impl OverTimeUdf {
    pub(crate) fn new(family: OverTimeFamily) -> Self {
        Self {
            family,
            // Args mix scalars and Dictionary range columns, so coercion is
            // bespoke: accept whatever the planner supplies, validate at invoke.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Returns the positional-argument count this family expects.
    pub(crate) fn arity(&self) -> usize {
        if self.family.takes_quantile_param() {
            4
        } else {
            3
        }
    }
}

impl ScalarUDFImpl for OverTimeUdf {
    fn name(&self) -> &str {
        self.family.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    /// `Signature::user_defined` needs its own coercion.
    ///
    /// These UDFs accept their arguments unchanged. The `RangeArray` dictionary
    /// columns and the scalars are already the exact types the planner supplies.
    /// This method checks the arity and returns the types unchanged.
    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        if arg_types.len() != self.arity() {
            return Err(DataFusionError::Plan(format!(
                "{} expects {} arguments, got {}",
                self.name(),
                self.arity(),
                arg_types.len()
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let name = self.name();
        if args.args.len() != self.arity() {
            return Err(DataFusionError::Execution(format!(
                "{name} expects {} arguments, got {}",
                self.arity(),
                args.args.len()
            )));
        }
        let rows = args.number_rows;

        // For the quantile family the leading argument is the `phi` scalar; the
        // three windowed columns follow. For every other family the windowed
        // columns start at index 0.
        let (phi, base) = if self.family.takes_quantile_param() {
            (scalar_f64(&args.args[0], "phi", name)?, 1)
        } else {
            (f64::NAN, 0)
        };

        let windows = WindowColumns::decode(&args.args[base..], rows, name)?;
        windows.check_rows(rows, name)?;

        let mut builder = Float64Builder::with_capacity(rows);
        for row in 0..rows {
            let (timestamps, values) = windows.window(row, name)?;
            match self.family.eval_window(timestamps, values, phi) {
                // A genuinely-computed reduction (including a legitimately-NaN
                // result, e.g. a quantile over a NaN sample) is kept as a non-null
                // float so it propagates through downstream aggregates exactly as
                // the interpreter propagates it.
                Some(value) => builder.append_value(value),
                // Empty window: Prometheus emits no sample. Emit NULL — not a NaN
                // sentinel — so the assembler drops the series and aggregates skip
                // it, matching the interpreter, which omits no-value series before
                // aggregating.
                None => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Returns the `over_time` UDF for `family`.
#[must_use]
pub fn over_time_udf(family: OverTimeFamily) -> ScalarUDF {
    ScalarUDF::from(OverTimeUdf::new(family))
}

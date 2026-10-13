use super::{
    super::{UdfArityPhase, check_udf_arity},
    ColumnarValue, DataType, DfResult, OverTimeFamily, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, WindowColumns, append_window_values,
    impl_window_udf_identity, scalar_f64,
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
    impl_window_udf_identity!();

    /// `Signature::user_defined` needs its own coercion.
    ///
    /// These UDFs accept their arguments unchanged. The `RangeArray` dictionary
    /// columns and the scalars are already the exact types the planner supplies.
    /// This method checks the arity and returns the types unchanged.
    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        check_udf_arity(
            self.name(),
            self.arity(),
            arg_types.len(),
            UdfArityPhase::Plan,
        )?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let name = self.name();
        check_udf_arity(
            name,
            self.arity(),
            args.args.len(),
            UdfArityPhase::Execution,
        )?;
        let rows = args.number_rows;

        // For the quantile family the leading argument is the `phi` scalar; the
        // three windowed columns follow. For every other family the windowed
        // columns start at index 0.
        let base = usize::from(self.family.takes_quantile_param());
        let phi = if self.family.takes_quantile_param() {
            scalar_f64(&args.args[0], "phi", name)?
        } else {
            f64::NAN
        };

        let windows = WindowColumns::decode(&args.args[base..], rows, name)?;
        windows.check_rows(rows, name)?;

        append_window_values(rows, |row| {
            let window_values = windows.window(row, name)?;
            Ok(self
                .family
                .eval_window(window_values.timestamps, window_values.values, phi))
        })
    }
}

/// Returns the `over_time` UDF for `family`.
#[must_use]
pub fn over_time_udf(family: OverTimeFamily) -> ScalarUDF {
    ScalarUDF::from(OverTimeUdf::new(family))
}

use super::{
    Arc, Array, ArrayRef, ColumnarValue, DataFusionError, DataType, DfResult, Float64Array,
    Float64Builder, ScalarFunctionArgs, ScalarMathOp, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, scalar_f64,
};

/// A `ScalarUDFImpl` over the inner instant vector's `value` column.
///
/// The call can add leading scalar bound columns. There is one instance per
/// [`ScalarMathOp`].
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ScalarMathUdf {
    pub(crate) op: ScalarMathOp,
    pub(crate) signature: Signature,
}

impl ScalarMathUdf {
    pub(crate) fn new(op: ScalarMathOp) -> Self {
        Self {
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ScalarMathUdf {
    fn name(&self) -> &str {
        self.op.udf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    /// `Signature::user_defined` needs its own type coercion.
    ///
    /// Every argument is a `Float64` column: the value plus any leading scalars.
    /// This method checks the arity and returns the types unchanged.
    fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
        if arg_types.len() != self.op.arity() {
            return Err(DataFusionError::Plan(format!(
                "{} expects {} arguments, got {}",
                self.name(),
                self.op.arity(),
                arg_types.len()
            )));
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let name = self.name();
        if args.args.len() != self.op.arity() {
            return Err(DataFusionError::Execution(format!(
                "{name} expects {} arguments, got {}",
                self.op.arity(),
                args.args.len()
            )));
        }
        let rows = args.number_rows;
        let params = self.op.scalar_param_count();

        // The leading scalar bound columns (round's `to_nearest`, clamp's
        // bounds) are constant per query; read each once.
        let mut bounds = Vec::with_capacity(params);
        for index in 0..params {
            bounds.push(scalar_f64(&args.args[index], "bound", name)?);
        }

        // The value column trails the scalar params.
        let value_array = args.args[params].clone().into_array(rows)?;
        let values = value_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{name}: `value` must be Float64, got {:?}",
                    value_array.data_type()
                ))
            })?;
        if values.len() != rows {
            return Err(DataFusionError::Execution(format!(
                "{name}: row-count mismatch (value={}, rows={rows})",
                values.len()
            )));
        }

        let mut builder = Float64Builder::with_capacity(rows);
        for row in 0..rows {
            // Genuine NaN (including a null cell, treated as NaN) flows through
            // unchanged; the interpreter never drops a float sample here.
            let value = if values.is_null(row) {
                f64::NAN
            } else {
                values.value(row)
            };
            builder.append_value(self.op.apply(value, &bounds));
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// The scalar-math UDF for `op`.
#[must_use]
pub fn scalar_math_udf(op: ScalarMathOp) -> ScalarUDF {
    ScalarUDF::from(ScalarMathUdf::new(op))
}

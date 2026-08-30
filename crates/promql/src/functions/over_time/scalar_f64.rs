use super::{ColumnarValue, DfResult, ScalarValue, DataFusionError, Array, Float64Array};

/// Reads a scalar `Float64` argument, or a single-row array as a fallback.
pub(crate) fn scalar_f64(value: &ColumnarValue, arg: &str, udf: &str) -> DfResult<f64> {
    match value {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Float64(Some(v)) => Ok(*v),
            other => Err(DataFusionError::Execution(format!(
                "{udf}: `{arg}` must be a non-null Float64 scalar, got {other:?}"
            ))),
        },
        ColumnarValue::Array(array) => {
            let floats = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{udf}: `{arg}` must be Float64, got {:?}",
                        array.data_type()
                    ))
                })?;
            if floats.is_empty() || floats.is_null(0) {
                return Err(DataFusionError::Execution(format!(
                    "{udf}: `{arg}` must be a non-null Float64"
                )));
            }
            Ok(floats.value(0))
        }
    }
}

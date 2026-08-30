use super::*;

/// Reads a scalar `Int64` argument, with a single-row array as a fallback.
pub(crate) fn scalar_i64(value: &ColumnarValue, arg: &str, udf: &str) -> DfResult<i64> {
    match value {
        ColumnarValue::Scalar(scalar) => match scalar {
            datafusion::common::ScalarValue::Int64(Some(v)) => Ok(*v),
            other => Err(DataFusionError::Execution(format!(
                "{udf}: `{arg}` must be a non-null Int64 scalar, got {other:?}"
            ))),
        },
        ColumnarValue::Array(array) => {
            let ints = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{udf}: `{arg}` must be Int64, got {:?}",
                    array.data_type()
                ))
            })?;
            if ints.is_empty() || ints.is_null(0) {
                return Err(DataFusionError::Execution(format!(
                    "{udf}: `{arg}` must be a non-null Int64"
                )));
            }
            Ok(ints.value(0))
        }
    }
}

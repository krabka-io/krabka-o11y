//! Argument decoding shared by the range-vector and scalar-math UDFs.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int64Array},
    datatypes::Int64Type,
};
use datafusion::{
    common::{DataFusionError, Result as DfResult, ScalarValue},
    logical_expr::ColumnarValue,
};

use crate::range_array::RangeArray;

pub(crate) struct WindowValues<'a> {
    pub timestamps: &'a [i64],
    pub values: &'a [f64],
}

/// Decodes a `Dictionary<Int64, List<_>>` range column into a [`RangeArray`].
pub(crate) fn decode_range_column(array: &ArrayRef, arg: &str, udf: &str) -> DfResult<RangeArray> {
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{udf}: `{arg}` must be a RangeArray dictionary column, got {:?}",
                array.data_type()
            ))
        })?;
    RangeArray::try_from_dict_array(dict)
        .map_err(|error| DataFusionError::Execution(format!("{udf}: decoding `{arg}`: {error}")))
}

/// Appends each window's result, preserving `None` as a null Arrow value.
pub(crate) fn append_window_values(
    rows: usize,
    mut evaluate: impl FnMut(usize) -> DfResult<Option<f64>>,
) -> DfResult<ColumnarValue> {
    let mut builder = Float64Builder::with_capacity(rows);
    for row in 0..rows {
        match evaluate(row)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
}

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

/// The eval-timestamp column and the windowed timestamp and value range
/// columns of one range UDF call.
pub(crate) struct WindowColumns {
    /// `range_end_ms` per step.
    pub(crate) eval_ts: Int64Array,
    pub(crate) timestamp_range: RangeArray,
    pub(crate) value_range: RangeArray,
}

impl WindowColumns {
    /// Decodes the three window columns that lead `window_args`, `rows` long,
    /// for the UDF called `udf`.
    pub(crate) fn decode(window_args: &[ColumnarValue], rows: usize, udf: &str) -> DfResult<Self> {
        let eval_ts = window_args[0].clone().into_array(rows)?;
        let eval_ts = eval_ts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{udf}: `eval_timestamp` must be Int64, got {:?}",
                    eval_ts.data_type()
                ))
            })?
            .clone();
        let timestamp_range = window_args[1].clone().into_array(rows)?;
        let timestamp_range = decode_range_column(&timestamp_range, "timestamp_range", udf)?;
        let value_range = window_args[2].clone().into_array(rows)?;
        let value_range = decode_range_column(&value_range, "value_range", udf)?;
        Ok(Self {
            eval_ts,
            timestamp_range,
            value_range,
        })
    }

    /// Rejects columns whose lengths are not all `rows`.
    pub(crate) fn check_rows(&self, rows: usize, udf: &str) -> DfResult<()> {
        if self.timestamp_range.len() != rows
            || self.value_range.len() != rows
            || self.eval_ts.len() != rows
        {
            return Err(DataFusionError::Execution(format!(
                "{udf}: row-count mismatch (eval_ts={}, timestamp_range={}, value_range={}, rows={rows})",
                self.eval_ts.len(),
                self.timestamp_range.len(),
                self.value_range.len()
            )));
        }
        Ok(())
    }

    /// The timestamps and values of window `row`.
    pub(crate) fn window(&self, row: usize, udf: &str) -> DfResult<WindowValues<'_>> {
        let timestamps = self.timestamp_range.timestamp_slice(row).ok_or_else(|| {
            DataFusionError::Execution(format!("{udf}: `timestamp_range` cell {row} is not Int64"))
        })?;
        let values = self.value_range.value_slice(row).ok_or_else(|| {
            DataFusionError::Execution(format!("{udf}: `value_range` cell {row} is not Float64"))
        })?;
        Ok(WindowValues { timestamps, values })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;

    use super::*;

    /// Confirms that the helper round-trips a `DictionaryArray` into a
    /// `RangeArray`, and rejects a non-dictionary column, for both the
    /// rate-family and the over-time UDFs that call it.
    #[test]
    fn decode_range_column_round_trips() {
        for udf in ["prom_rate", "prom_sum_over_time"] {
            let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
            let range = RangeArray::from_ranges(
                values,
                [crate::RangeWindow::new(0, 2), crate::RangeWindow::new(2, 1)],
            )
            .unwrap();
            let dict: ArrayRef = Arc::new(range.into_dict_array().unwrap());
            let back = decode_range_column(&dict, "value_range", udf).unwrap();
            check!(back.len() == 2);
            check!(back.value_slice(0).unwrap() == [1.0, 2.0]);

            // A non-dictionary column is rejected.
            let plain: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
            check!(decode_range_column(&plain, "value_range", udf).is_err());
        }
    }
}

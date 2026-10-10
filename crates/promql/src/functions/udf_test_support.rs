//! Window builders and result readers shared by the range UDF unit tests.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use datafusion::logical_expr::ColumnarValue;

use crate::range_array::RangeArray;

/// One evaluation step of a range UDF: its eval timestamp and its window's
/// samples.
#[derive(Clone, Copy)]
pub(crate) struct WindowStep<'a> {
    pub(crate) eval_ts_ms: i64,
    pub(crate) timestamps: &'a [i64],
    /// One sample value per timestamp.
    pub(crate) sample_values: &'a [f64],
}

/// Flattens per-step windows into the eval-timestamp column and the timestamp-
/// and value-range dictionary columns a range UDF reads, in that order.
pub(crate) fn window_columns(steps: &[WindowStep<'_>]) -> [ArrayRef; 3] {
    let mut all_ts = Vec::new();
    let mut all_val = Vec::new();
    let mut ranges = Vec::new();
    let mut eval = Vec::new();
    let mut offset = 0_u32;
    for step in steps {
        assert2::assert!(step.timestamps.len() == step.sample_values.len());
        let len = u32::try_from(step.timestamps.len()).unwrap();
        all_ts.extend_from_slice(step.timestamps);
        all_val.extend_from_slice(step.sample_values);
        ranges.push((offset, len));
        offset += len;
        eval.push(step.eval_ts_ms);
    }
    let (value_ra, ts_ra) = RangeArray::from_paired_ranges(
        Float64Array::from(all_val),
        Int64Array::from(all_ts),
        ranges,
    )
    .unwrap();
    [
        Arc::new(Int64Array::from(eval)),
        Arc::new(ts_ra.into_dict_array().unwrap()),
        Arc::new(value_ra.into_dict_array().unwrap()),
    ]
}

/// Reads a UDF's `rows` output cells, with `None` for a NULL cell.
pub(crate) fn nullable_floats(out: ColumnarValue, rows: usize) -> Vec<Option<f64>> {
    let array = out.into_array(rows).unwrap();
    let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..floats.len())
        .map(|i| {
            if floats.is_null(i) {
                None
            } else {
                Some(floats.value(i))
            }
        })
        .collect()
}

/// Whether a UDF output agrees with the expected float within `1e-9`.
pub(crate) fn approx_eq(left: f64, right: f64) -> bool {
    (left - right).abs() < 1e-9
}

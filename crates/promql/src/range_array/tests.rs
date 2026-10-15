use std::sync::Arc;

use arrow::{
    array::{ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int64Array, StructArray},
    datatypes::{Field, Int64Type, Schema},
    record_batch::RecordBatch,
};
use assert2::check;
use krabka_metrics::{
    BucketSpan, NativeHistogram, NativeHistogramRow, ResetHint, decode_native_histograms,
    encode_native_histograms,
};

use super::{RangeArray, RangeWindow};

fn int_range_array() -> RangeArray {
    let timestamps = Arc::new(Int64Array::from(vec![0_i64, 15, 30, 45])) as ArrayRef;
    RangeArray::from_ranges(timestamps, [RangeWindow::new(0, 2), RangeWindow::new(2, 2)]).unwrap()
}

fn float_range_array() -> RangeArray {
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef;
    RangeArray::from_ranges(values, [RangeWindow::new(0, 2), RangeWindow::new(1, 3)]).unwrap()
}

mod native_histogram_rows;

mod accessor_cases;
mod histogram_cases;
mod window_builder_cases;
use native_histogram_rows::{native_histogram_rows, native_histogram_struct_array};
use window_floats::window_floats;

mod support;
pub(crate) use support::window_floats;

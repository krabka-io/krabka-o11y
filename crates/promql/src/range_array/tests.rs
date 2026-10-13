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

mod basic_accessors_report_empty_state_and_exact_ranges;
mod cell_len_and_empty_cells;
mod dict_array_round_trips_through_recordbatch_column;
mod histogram_cell_matches_get_over_a_pre_sliced_backing_array;
mod histogram_cell_reads_native_histogram_windows;
mod iter_float_cells_visits_every_window;
mod iter_int_cells_visits_every_window;
mod native_histogram_rows;
mod out_of_bounds_window_is_rejected;
mod paired_builder_rejects_length_mismatch;
mod paired_builder_shares_window_offsets_across_value_and_timestamp;
mod survives_datafusion_projection_as_a_column;
mod timestamp_slice_reads_typed_int_cells;
mod typed_accessor_matches_get_over_a_pre_sliced_backing_array;
mod value_slice_reads_typed_float_cells;
mod window_floats;
mod windows_slice_the_backing_array;

use native_histogram_rows::{native_histogram_rows, native_histogram_struct_array};
use window_floats::window_floats;

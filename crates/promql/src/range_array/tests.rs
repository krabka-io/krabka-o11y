use std::sync::Arc;

use arrow::{
    array::{ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int64Array, StructArray},
    datatypes::{Field, Int64Type, Schema},
    record_batch::RecordBatch,
};
use assert2::check;
use krabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, decode_native_histograms, encode_native_histograms,
};

use super::RangeArray;

// === split-modules: generated submodules ===
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
mod windows_slice_the_backing_array;

use native_histogram_rows::native_histogram_rows;

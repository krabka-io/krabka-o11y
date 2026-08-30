//! `RangeArray`: a list-like view where each cell is a sample window.
//!
//! # Native-histogram range vectors
//!
//! The windowing core is type-agnostic. It windows any backing [`ArrayRef`] by
//! `(offset, len)` and round-trips it as a dictionary-of-lists column. That core
//! is [`from_ranges`](RangeArray::from_ranges), [`get`](RangeArray::get),
//! [`into_dict_array`](RangeArray::into_dict_array), and
//! [`try_from_dict_array`](RangeArray::try_from_dict_array). A native-histogram
//! range vector already works structurally: back the `RangeArray` with a
//! `StructArray` instead of a `Float64Array`. That struct holds the count, sum,
//! and schema scalars plus the bucket-bound and bucket-count lists. `get(i)`
//! returns the sliced `StructArray` for that window.
//!
//! The typed fast-path accessors below cover scalar cells and native histogram
//! cells. Use [`value_slice`](RangeArray::value_slice) for `f64` cells,
//! [`timestamp_slice`](RangeArray::timestamp_slice) for `i64` cells, and
//! [`histogram_cell`](RangeArray::histogram_cell) for native histogram cells.

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int8Array, Int32Array,
        Int64Array, ListArray, StructArray, UInt32Array,
    },
    buffer::{OffsetBuffer, ScalarBuffer},
    compute::concat,
    datatypes::{Field, Int64Type},
    error::ArrowError,
};
use krabka_metrics::{
    COL_NH_COUNT, COL_NH_CUSTOM_VALUES, COL_NH_IS_FLOAT, COL_NH_NEG_COUNTS, COL_NH_NEG_SPANS,
    COL_NH_POS_COUNTS, COL_NH_POS_SPANS, COL_NH_RESET_HINT, COL_NH_SCHEMA, COL_NH_START_TS,
    COL_NH_SUM, COL_NH_ZERO_COUNT, COL_NH_ZERO_THRESHOLD,
};

#[cfg(test)]
mod tests;

// === split-modules: generated submodules ===
mod f64_list_value;
mod histogram_columns;
mod histogram_span_view;
mod histogram_view;
mod list_offsets;
mod range_array;
mod span_list_value;
mod struct_column;

use f64_list_value::f64_list_value;
use histogram_columns::HistogramColumns;
pub use histogram_span_view::HistogramSpanView;
pub use histogram_view::HistogramView;
use list_offsets::list_offsets;
pub use range_array::RangeArray;
use span_list_value::span_list_value;
use struct_column::struct_column;

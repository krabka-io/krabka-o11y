use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use arrow::{
    array::{Array, AsArray},
    datatypes::{Float64Type, Int64Type, UInt64Type},
};
use datafusion::prelude::SessionContext;
use krabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};
use krabka_metrics::{NativeHistogram, decode_native_histograms};

use super::samples_per_query_exceeded;
use crate::{PromqlError, ScanResult, error::Result};

tokio::task_local! {
    /// Active for the dynamic extent of the step loop in
    /// `PromqlEngine::eval_range_via_planner` and of one instant query in
    /// `PromqlEngine::query_instant_with_annotations`. Nested range evaluations
    /// (subqueries) shadow it with their own cache and restore the outer cache
    /// on exit, so each range scans its own union.
    pub(super) static RANGE_SCAN_CACHE: RangeScanCache;
}

mod collect_float_rows;
mod collect_histogram_rows;
mod float_row;
mod float_window;
mod histogram_row;
mod matchers_cache_key;
mod range_scan_cache;
mod range_scan_cache_inner;
mod scanned_rows;
mod through_scan_cache;
mod windowed;

pub(super) use collect_float_rows::collect_float_rows;
pub(super) use collect_histogram_rows::collect_histogram_rows;
pub(super) use float_row::FloatRow;
pub(super) use float_window::FloatWindow;
pub(super) use histogram_row::HistogramRow;
pub(super) use matchers_cache_key::matchers_cache_key;
pub(super) use range_scan_cache::RangeScanCache;
pub(super) use range_scan_cache_inner::RangeScanCacheInner;
pub(super) use scanned_rows::ScannedRows;
pub(super) use through_scan_cache::through_scan_cache;
pub(super) use windowed::Windowed;

//! Scan-table merging for [`super::MergedMetricStore`].

use std::sync::Arc;

use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_metrics::{COL_FINGERPRINT, COL_TIMESTAMP};

use crate::PromqlError;
mod merge_scan_table;
pub(super) use merge_scan_table::{ScanTableInput, merge_scan_table};

pub(crate) const FLOAT_TABLE: &str = "merged_float_samples";

pub(crate) const HISTOGRAM_TABLE: &str = "merged_native_histograms";

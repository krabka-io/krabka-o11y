//! Scan-table merging for [`super::MergedMetricStore`].

use std::sync::Arc;

use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_metrics::{COL_FINGERPRINT, COL_TIMESTAMP};

use crate::PromqlError;

// === split-modules: generated submodules ===
mod float_table;
mod histogram_table;
mod merge_scan_table;
mod quote_ident;

pub(super) use float_table::FLOAT_TABLE;
pub(super) use histogram_table::HISTOGRAM_TABLE;
pub(super) use merge_scan_table::merge_scan_table;
use quote_ident::quote_ident;

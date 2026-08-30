//! In-memory `MetricStore` used by conformance and engine tests.

use std::collections::{BTreeMap, HashMap};

use krabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};
use krabka_metrics::NativeHistogram;
use krabka_units::prelude::*;

use crate::{
    error::Result,
    ids::{Offset, PartitionIndex},
    store::{MetadataRecord, TsdbBlock},
};

mod head;
mod ingest;
mod matcher;
mod store_impl;

pub use head::WalHead;
use matcher::{prepare_matchers, row_matches};

#[cfg(test)]
mod tests;

// === split-modules: generated submodules ===
mod default_retention;
mod exemplar_row;
mod float_row;
mod hist_row;
mod in_memory_metric_store;
mod partition_watermark;
mod prune_stats;

pub use default_retention::DEFAULT_RETENTION;
use exemplar_row::ExemplarRow;
use float_row::FloatRow;
use hist_row::HistRow;
pub use in_memory_metric_store::InMemoryMetricStore;
pub use partition_watermark::PartitionWatermark;
pub use prune_stats::PruneStats;

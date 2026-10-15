//! In-memory `MetricStore` used by conformance and engine tests.

use std::{collections::{BTreeMap, HashMap}, sync::Arc};

use krabka_blockstore::SeriesFingerprint;
use krabka_metrics::NativeHistogram;
use krabka_units::prelude::*;

use crate::{
    PromqlLabels as Labels, PromqlMatcher as LabelMatcher,
    error::Result,
    ids::{Offset, PartitionIndex},
    store::{MetadataRecord, TsdbBlock},
};

mod head;
mod ingest;
pub(crate) mod matcher;
mod store_impl;

pub use head::{WalHead, WalRecordAt};
pub(crate) use matcher::{prepare_matchers, row_matches};

#[cfg(test)]
mod tests;

mod float_head_summary;
mod in_memory_metric_store;
mod row_chunks;
mod series_sample_ref;

pub use in_memory_metric_store::InMemoryMetricStore;
use row_chunks::RowChunks;
use series_sample_ref::SeriesSampleRef;

use self::float_head_summary::FloatHeadSummary;

/// Default head retention window: the head keeps six hours of samples hot.
pub const DEFAULT_RETENTION: Time = hours(6);

#[derive(Clone)]
pub(crate) struct ExemplarRow {
    /// Shared, and cloned by pointer when the head is copied.
    pub(crate) series_labels: Arc<Labels>,
    pub(crate) labels: Arc<Labels>,
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
}

#[derive(Clone)]
pub(crate) struct FloatRow {
    pub(crate) fp: SeriesFingerprint,
    /// Shared with every other row of the same series in the record that
    /// produced it, and cloned by pointer when the head is copied.
    pub(crate) labels: Arc<Labels>,
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
    pub(crate) start_timestamp_ms: Option<i64>,
}

#[derive(Clone)]
pub(crate) struct HistRow {
    pub(crate) fp: SeriesFingerprint,
    /// Shared, and cloned by pointer when the head is copied.
    pub(crate) labels: Arc<Labels>,
    pub(crate) ts_ms: i64,
    /// Behind an `Arc` because a histogram owns four vectors, and head clones
    /// should share those vectors.
    pub(crate) hist: Arc<NativeHistogram>,
}

/// WAL offset watermarks materialized in the head for one partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionWatermark {
    /// First WAL offset the store ingested into the head for this partition.
    pub low_water_offset: Offset,
    /// Most recent WAL offset the store ingested into the head for this partition.
    pub high_water_offset: Offset,
}

/// Counts of what an [`crate::InMemoryMetricStore::prune`] pass evicted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PruneStats {
    pub samples_dropped: usize,
    pub series_dropped: usize,
}

/// Rows per sealed chunk in a [`RowChunks`].
pub(crate) const ROW_CHUNK_LEN: usize = 1_024;

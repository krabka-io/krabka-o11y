//! In-memory `MetricStore` used by conformance and engine tests.

use std::collections::{BTreeMap, HashMap};

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

pub use head::WalHead;
pub(crate) use matcher::{prepare_matchers, row_matches};

#[cfg(test)]
mod tests;

mod default_retention;
mod exemplar_row;
mod float_head_summary;
mod float_row;
mod hist_row;
mod in_memory_metric_store;
mod partition_watermark;
mod prune_stats;
mod row_chunk_len;
mod row_chunks;
mod series_sample_ref;

pub use default_retention::DEFAULT_RETENTION;
use exemplar_row::ExemplarRow;
use float_row::FloatRow;
use hist_row::HistRow;
pub use in_memory_metric_store::InMemoryMetricStore;
pub use partition_watermark::PartitionWatermark;
pub use prune_stats::PruneStats;
use row_chunk_len::ROW_CHUNK_LEN;
use row_chunks::RowChunks;
use series_sample_ref::SeriesSampleRef;

use self::float_head_summary::FloatHeadSummary;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_blockstore::SeriesFingerprint;
use krabka_metrics::{
    encode_float_samples, encode_native_histograms, float_sample_schema, native_histogram_schema,
};

use super::{
    InMemoryMetricStore, RowChunks,
    matcher::{all_match, prepare_matchers, row_matches},
};
use crate::{
    PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher,
    error::Result,
    store::{
        ExemplarRecord, ExemplarScan, LabelNameCardinality, LabelValueCardinality, MetadataScan,
        MetricStore, NamedTsdbStat, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
    },
};

mod in_memory_metric_store;
mod named_stats;

use named_stats::named_stats;

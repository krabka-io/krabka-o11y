use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_blockstore::{LabelMatcher, Labels, SeriesFingerprint};
use krabka_metrics::{
    encode_float_samples, encode_native_histograms, float_sample_schema, native_histogram_schema,
};

use super::{
    InMemoryMetricStore,
    matcher::{all_match, prepare_matchers, row_matches},
};
use crate::{
    PromqlError,
    error::Result,
    store::{
        ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
        NamedTsdbStat, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
    },
};

// === split-modules: generated submodules ===
mod in_memory_metric_store;
mod named_stats;

use named_stats::named_stats;

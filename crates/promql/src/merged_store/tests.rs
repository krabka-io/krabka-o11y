use std::sync::Arc;

use krabka_blockstore::Labels;

use crate::{
    EngineOpts, ExemplarRecord, InMemoryMetricStore, InstantSample, MergedMetricStore, MetricStore,
    NamedTsdbStat, PromqlEngine, QueryResult, SampleValue, TsdbHeadStats, TsdbStats,
};

// === split-modules: generated submodules ===
mod cardinality_methods_merge_cold_and_hot_series;
mod exemplars_merges_cold_and_hot_records;
mod instant_query_uses_hot_sample_newer_than_compacted_sample;
mod label_names_merges_cold_and_hot_series_metadata;
mod label_values_merges_cold_and_hot_series_metadata;
mod labels;
mod metadata_merges_cold_and_hot_records;
mod min_present_time_preserves_legitimate_zero_min_time;
mod range_query_counts_sample_present_in_both_stores_once;
mod tsdb_blocks_merges_cold_and_hot_blocks;
mod tsdb_stats_ignore_empty_side_min_time;
mod tsdb_stats_merge_cold_and_hot_counts;

use labels::labels;

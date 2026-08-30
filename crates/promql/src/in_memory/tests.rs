use arrow::{array::AsArray, datatypes::Int64Type};
use assert2::check;
use krabka_blockstore::{LabelMatcher, Labels, MatchOp};
use krabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, SamplePayload, WalExemplar, WalRecord,
};

use super::*;
use crate::{
    EngineOpts, PromqlEngine, PromqlError, QueryResult, SampleValue, WalHead,
    store::{
        LabelNameCardinality, LabelValueCardinality, MetricStore, NamedTsdbStat, ScanResult,
        TsdbHeadStats,
    },
};

// === split-modules: generated submodules ===
mod bulk_wal_replay_and_retention_are_observable;
mod cloned_wal_head_sees_records_replayed_through_original_handle;
mod count_rows;
mod expected_label_memory_stats;
mod expected_label_name_cardinality;
mod expected_label_pair_stats;
mod expected_label_value_cardinality;
mod expected_label_value_count_stats;
mod expected_metric_name_stats;
mod float_record;
mod label_values_returns_distinct_for_name;
mod lbls;
mod native_histogram;
mod offsets_track_low_and_high_water;
mod prune_counts_partial_histogram_and_exemplar_retention;
mod prune_drops_old_samples;
mod prune_removes_emptied_series_from_index;
mod query_shard_matcher_filters_by_series_fingerprint_modulo;
mod query_shard_neq_matcher_excludes_matching_fingerprint_modulo;
mod regex_matchers_are_anchored_and_absent_labels_match_empty;
mod replay_wal_records_populates_queryable_head;
mod row_matches_rejects_outside_bounds_before_matching_labels;
mod scan_filters_by_matcher_and_time_and_registers_float_table;
mod scan_filters_histograms_by_matcher_tenant_and_time;
mod scan_validates_regex_matchers_before_row_iteration;
mod scan_with_no_match_returns_none_tables;
mod series_filters_histograms_by_matcher_and_time;
mod store_cardinality_and_tsdb_stats_include_float_and_hist_series;
mod store_with_float_and_hist_series;
mod wal_head_delegates_metadata_cardinality_stats_and_blocks;

use count_rows::count_rows;
use expected_label_memory_stats::expected_label_memory_stats;
use expected_label_name_cardinality::expected_label_name_cardinality;
use expected_label_pair_stats::expected_label_pair_stats;
use expected_label_value_cardinality::expected_label_value_cardinality;
use expected_label_value_count_stats::expected_label_value_count_stats;
use expected_metric_name_stats::expected_metric_name_stats;
use float_record::float_record;
use lbls::lbls;
use native_histogram::native_histogram;
use store_with_float_and_hist_series::store_with_float_and_hist_series;

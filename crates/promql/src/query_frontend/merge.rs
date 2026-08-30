use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use krabka_blockstore::{Labels, SeriesFingerprint};
use krabka_metrics::NativeHistogram;
use promql_parser::parser::LabelModifier;

use super::{MomentReduction, QueryShardReducer, RankReduction};
use crate::{
    PromqlError, QueryResult, RangeSeries, SampleValue, engine::add_compatible_native_histogram,
};

// === split-modules: generated submodules ===
mod aggregate_labels;
mod compare_rank_candidates;
mod divide_range_query_results;
mod float_samples_by_fingerprint;
mod label_sort_key;
mod merge_range_query_results;
mod merge_range_query_results_with_reducer;
mod rank_candidate;
mod reduce_duplicate_step_samples;
mod reduce_moment_range_query_results;
mod reduce_rank_range_query_results;
mod scaled_native_histogram;

use aggregate_labels::aggregate_labels;
use compare_rank_candidates::compare_rank_candidates;
pub(super) use divide_range_query_results::divide_range_query_results;
use float_samples_by_fingerprint::float_samples_by_fingerprint;
use label_sort_key::label_sort_key;
pub use merge_range_query_results::merge_range_query_results;
pub(super) use merge_range_query_results_with_reducer::merge_range_query_results_with_reducer;
use rank_candidate::RankCandidate;
use reduce_duplicate_step_samples::reduce_duplicate_step_samples;
pub(super) use reduce_moment_range_query_results::reduce_moment_range_query_results;
pub(super) use reduce_rank_range_query_results::reduce_rank_range_query_results;
use scaled_native_histogram::scaled_native_histogram;

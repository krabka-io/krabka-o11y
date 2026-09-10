use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

use crate::result::Annotations;

tokio::task_local! {
    /// Per-query annotation sink.
    ///
    /// Each public query entry point scopes this once. The deeply recursive
    /// evaluation path can then record warnings and infos without a collector
    /// argument at every call site.
    pub(crate) static ANNOTATIONS: RefCell<Annotations>;
}

mod bad_bucket_label_warning;
mod emit_info;
mod emit_warning;
mod histogram_counter_reset_collision_warning;
mod histogram_ignored_in_aggregation_info;
mod histogram_ignored_in_mixed_range_info;
mod histogram_quantile_forced_monotonicity_info;
mod incompatible_types_in_binop_info;
mod invalid_quantile_warning;
mod invalid_ratio_warning;
mod is_valid_quantile;
mod maybe_with_metric_name;
mod mismatched_custom_buckets_info;
mod mixed_classic_native_warning;
mod mixed_exponential_custom_warning;
mod mixed_floats_histograms_agg_warning;
mod mixed_floats_histograms_warning;
mod native_histogram_fraction_nans_info;
mod native_histogram_not_counter_warning;
mod native_histogram_not_gauge_warning;
mod native_histogram_quantile_nan_result_info;
mod native_histogram_quantile_nan_skew_info;
mod warn_mixed_histograms;

pub(super) use bad_bucket_label_warning::bad_bucket_label_warning;
pub(super) use emit_info::emit_info;
pub(super) use emit_warning::emit_warning;
pub(super) use histogram_counter_reset_collision_warning::histogram_counter_reset_collision_warning;
pub(super) use histogram_ignored_in_aggregation_info::histogram_ignored_in_aggregation_info;
pub(super) use histogram_ignored_in_mixed_range_info::histogram_ignored_in_mixed_range_info;
pub(super) use histogram_quantile_forced_monotonicity_info::histogram_quantile_forced_monotonicity_info;
pub(super) use incompatible_types_in_binop_info::incompatible_types_in_binop_info;
pub(super) use invalid_quantile_warning::invalid_quantile_warning;
#[cfg(feature = "experimental-functions")]
pub(super) use invalid_ratio_warning::invalid_ratio_warning;
pub(super) use is_valid_quantile::is_valid_quantile;
use maybe_with_metric_name::maybe_with_metric_name;
pub(super) use mismatched_custom_buckets_info::mismatched_custom_buckets_info;
use mixed_classic_native_warning::mixed_classic_native_warning;
pub(super) use mixed_exponential_custom_warning::mixed_exponential_custom_warning;
pub(super) use mixed_floats_histograms_agg_warning::mixed_floats_histograms_agg_warning;
pub(super) use mixed_floats_histograms_warning::mixed_floats_histograms_warning;
pub(super) use native_histogram_fraction_nans_info::native_histogram_fraction_nans_info;
pub(super) use native_histogram_not_counter_warning::native_histogram_not_counter_warning;
pub(super) use native_histogram_not_gauge_warning::native_histogram_not_gauge_warning;
pub(super) use native_histogram_quantile_nan_result_info::native_histogram_quantile_nan_result_info;
pub(super) use native_histogram_quantile_nan_skew_info::native_histogram_quantile_nan_skew_info;
pub(super) use warn_mixed_histograms::warn_mixed_histograms;

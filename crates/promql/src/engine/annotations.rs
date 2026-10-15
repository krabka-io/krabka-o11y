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

mod annotation_source;
mod counter_info;
mod histogram_quantile_forced_monotonicity_info;
mod range_sort_warnings;

pub(super) use annotation_source::{ANNOTATION_SOURCE, with_source_position};
pub(super) use histogram_quantile_forced_monotonicity_info::emit_histogram_quantile_forced_monotonicity_info;
pub(super) use counter_info::emit_metric_might_not_be_counter_info;
pub(super) use range_sort_warnings::emit_range_sort_warnings;
pub(super) use counter_info::TypeAndUnitLabels;

/// Records a `PromQL info:`-class annotation for the current query.
///
/// This function does nothing if no sink is in scope. See `emit_warning`.
pub(crate) fn emit_info(message: impl Into<String>) {
    let message = with_source_position(message.into());
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().info(message));
}

/// Records a `PromQL warning:`-class annotation for the current query.
///
/// This function does nothing if no sink is in scope. A unit test that calls
/// internals directly is outside a scoped query, so a call is always safe.
pub(crate) fn emit_warning(message: impl Into<String>) {
    let message = with_source_position(message.into());
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().warn(message));
}

/// Exact Prometheus `HistogramCounterResetCollisionWarning` text.
///
/// `operation` is Prometheus' own `HistogramOperation` word: `addition`,
/// `subtraction`, or `aggregation`.
pub(crate) fn histogram_counter_reset_collision_warning(operation: &str) -> String {
    format!("PromQL warning: conflicting counter resets during histogram {operation}")
}

/// Exact Prometheus `HistogramIgnoredInAggregationInfo` text for `aggregation`.
///
/// `aggregation` is the operator's own name: `min`, `max`, `stddev`, `stdvar`,
/// `quantile`, `topk`, or `bottomk`.
pub(crate) fn histogram_ignored_in_aggregation_info(aggregation: &str) -> String {
    format!("PromQL info: ignored histogram in {aggregation} aggregation")
}

/// Exact Prometheus `HistogramIgnoredInMixedRangeInfo` text for `metric`.
pub(crate) fn histogram_ignored_in_mixed_range_info(metric: &str) -> String {
    format!(
        "PromQL info: ignored histograms in a range containing both floats and histograms for metric name {metric:?}"
    )
}

/// Exact Prometheus `InvalidRatioWarning` text.
///
/// Rust's `f64` `Display` matches Go's `%g` for the integral and one-decimal
/// ratios this annotation reports: `1` for `1.0`, `1.1` for `1.1`, and `-1` for
/// `-1.0`. The rendered text is then byte-for-byte the corpus-asserted text.
#[cfg(feature = "experimental-functions")]
pub(crate) fn invalid_ratio_warning(got: f64, capped_to: f64) -> String {
    format!(
        "PromQL warning: ratio value should be between -1 and 1, got {got}, capping to {capped_to}"
    )
}

/// Returns true if `phi` is a valid quantile in `[0, 1]`.
///
/// The engine still evaluates an out-of-range or NaN phi. Prometheus returns
/// `+/-Inf` or `NaN` with an `InvalidQuantileWarning` and does not error. This
/// function only gates the warning.
pub(crate) fn is_valid_quantile(phi: f64) -> bool {
    (0.0..=1.0).contains(&phi)
}

pub(crate) fn maybe_add_metric_name(message: String, metric: &str) -> String {
    if metric.is_empty() {
        message
    } else {
        format!("{message} for metric name {metric:?}")
    }
}

/// Exact Prometheus `MismatchedCustomBucketsHistogramsInfo` text for `operation`.
///
/// `operation` is Prometheus' own `HistogramOperation` word: `addition`,
/// `subtraction`, or `aggregation`.
pub(crate) fn mismatched_custom_buckets_info(operation: &str) -> String {
    format!("PromQL info: mismatched custom buckets were reconciled during {operation}")
}

/// Exact Prometheus `MixedClassicNativeHistogramsWarning` text.
pub(crate) fn mixed_classic_native_warning(metric: &str) -> String {
    maybe_add_metric_name(
        "PromQL warning: vector contains a mix of classic and native histograms".to_string(),
        metric,
    )
}

/// Exact Prometheus `MixedExponentialCustomHistogramsWarning` text for `metric`.
pub(crate) fn mixed_exponential_custom_warning(metric: &str) -> String {
    format!(
        "PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name {metric:?}"
    )
}

/// Exact Prometheus `MixedFloatsHistogramsAggWarning` text.
pub(crate) fn mixed_floats_histograms_agg_warning() -> String {
    "PromQL warning: encountered a mix of histograms and floats for aggregation".to_string()
}

/// Exact Prometheus `MixedFloatsHistogramsWarning` text for `metric`.
pub(crate) fn mixed_floats_histograms_warning(metric: &str) -> String {
    format!("PromQL warning: encountered a mix of histograms and floats for metric name {metric:?}")
}

/// Exact Prometheus `NativeHistogramFractionNaNsInfo` text for `metric`.
pub(crate) fn native_histogram_fraction_nans_info(metric: &str) -> String {
    maybe_add_metric_name("PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions".to_string(), metric)
}

/// Exact Prometheus `NativeHistogramNotCounterWarning` text for `metric`.
pub(crate) fn native_histogram_not_counter_warning(metric: &str) -> String {
    format!("PromQL warning: this native histogram metric is not a counter: {metric:?}")
}

/// Exact Prometheus `NativeHistogramNotGaugeWarning` text for `metric`.
pub(crate) fn native_histogram_not_gauge_warning(metric: &str) -> String {
    format!("PromQL warning: this native histogram metric is not a gauge: {metric:?}")
}

/// Exact Prometheus `NativeHistogramQuantileNaNResultInfo` text for `metric`.
pub(crate) fn native_histogram_quantile_nan_result_info(metric: &str) -> String {
    maybe_add_metric_name(
        "PromQL info: input to histogram_quantile has NaN observations, result is NaN".to_string(),
        metric,
    )
}

/// Exact Prometheus `NativeHistogramQuantileNaNSkewInfo` text for `metric`.
pub(crate) fn native_histogram_quantile_nan_skew_info(metric: &str) -> String {
    maybe_add_metric_name(
        "PromQL info: input to histogram_quantile has NaN observations, result is skewed higher"
            .to_string(),
        metric,
    )
}

/// Exact Prometheus `BadBucketLabelWarning` text for `metric`.
///
/// `label` is the `le` value that could not be read, which is the empty string
/// where the series carries no `le` at all.
pub(crate) fn bad_bucket_label_warning(label: &str, metric: &str) -> String {
    maybe_add_metric_name(
        format!(
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of {label:?}"
        ),
        metric,
    )
}

/// Exact Prometheus `IncompatibleTypesInBinOpInfo` text for incompatible operands.
///
/// An operator gets incompatible operand sample types, for example a histogram
/// and a float.
pub(crate) fn incompatible_types_in_binop_info(
    lhs_type: &str,
    operator: &str,
    rhs_type: &str,
) -> String {
    format!(
        "PromQL info: incompatible sample types encountered for binary operator {operator:?}: {lhs_type} {operator} {rhs_type}"
    )
}

/// Exact Prometheus `InvalidQuantileWarning` text for a bad phi.
///
/// A bad phi is a `quantile` or `quantile_over_time` phi outside `[0, 1]`, or
/// NaN. Prometheus does not abort on a bad phi. It returns signed `+/-Inf` or
/// `NaN` and raises this warning, the same as the `histogram_quantile` family.
/// `got` renders through the canonical Prometheus float formatter, which
/// matches Go's `%v`.
pub(crate) fn invalid_quantile_warning(got: f64) -> String {
    format!(
        "PromQL warning: quantile value should be between 0 and 1, got {}",
        crate::http_api::format_sample_value(got)
    )
}

/// Emits one `MixedClassicNativeHistogramsWarning` per mixed group key.
///
/// A mixed group key held both a classic and a native histogram for the same
/// label set.
pub(crate) fn warn_mixed_histograms(
    mixed_keys: &BTreeSet<String>,
    names: &BTreeMap<String, String>,
) {
    for key in mixed_keys {
        let metric = names.get(key).map_or("", String::as_str);
        emit_warning(mixed_classic_native_warning(metric));
    }
}

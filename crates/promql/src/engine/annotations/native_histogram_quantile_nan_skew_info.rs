use super::maybe_with_metric_name;

/// Exact Prometheus `NativeHistogramQuantileNaNSkewInfo` text for `metric`.
pub(crate) fn native_histogram_quantile_nan_skew_info(metric: &str) -> String {
    maybe_with_metric_name(
        "PromQL info: input to histogram_quantile has NaN observations, result is skewed higher",
        metric,
    )
}

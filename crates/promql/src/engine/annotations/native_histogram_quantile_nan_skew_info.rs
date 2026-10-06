use super::maybe_add_metric_name;

/// Exact Prometheus `NativeHistogramQuantileNaNSkewInfo` text for `metric`.
pub(crate) fn native_histogram_quantile_nan_skew_info(metric: &str) -> String {
    maybe_add_metric_name(
        "PromQL info: input to histogram_quantile has NaN observations, result is skewed higher"
            .to_string(),
        metric,
    )
}

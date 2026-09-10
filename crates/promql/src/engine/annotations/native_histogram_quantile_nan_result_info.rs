use super::maybe_with_metric_name;

/// Exact Prometheus `NativeHistogramQuantileNaNResultInfo` text for `metric`.
pub(crate) fn native_histogram_quantile_nan_result_info(metric: &str) -> String {
    maybe_with_metric_name(
        "PromQL info: input to histogram_quantile has NaN observations, result is NaN",
        metric,
    )
}

/// Exact Prometheus `NativeHistogramQuantileNaNResultInfo` text for `metric`.
pub(crate) fn native_histogram_quantile_nan_result_info(_metric: &str) -> String {
    "PromQL info: input to histogram_quantile has NaN observations, result is NaN".to_string()
}

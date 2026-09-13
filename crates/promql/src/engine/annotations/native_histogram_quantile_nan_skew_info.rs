/// Exact Prometheus `NativeHistogramQuantileNaNSkewInfo` text for `metric`.
pub(crate) fn native_histogram_quantile_nan_skew_info(_metric: &str) -> String {
    "PromQL info: input to histogram_quantile has NaN observations, result is skewed higher"
        .to_string()
}

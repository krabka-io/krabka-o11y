/// Exact Prometheus `MixedExponentialCustomHistogramsWarning` text for `metric`.
pub(crate) fn mixed_exponential_custom_warning(metric: &str) -> String {
    format!(
        "PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name {metric:?}"
    )
}

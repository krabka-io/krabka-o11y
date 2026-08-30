
/// Exact Prometheus `MixedClassicNativeHistogramsWarning` text for `metric`.
pub(crate) fn mixed_classic_native_warning(metric: &str) -> String {
    format!(
        "PromQL warning: vector contains a mix of classic and native histograms for metric name {metric:?}"
    )
}

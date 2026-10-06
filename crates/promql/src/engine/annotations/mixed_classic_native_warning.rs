use super::maybe_add_metric_name;

/// Exact Prometheus `MixedClassicNativeHistogramsWarning` text.
pub(crate) fn mixed_classic_native_warning(metric: &str) -> String {
    maybe_add_metric_name(
        "PromQL warning: vector contains a mix of classic and native histograms".to_string(),
        metric,
    )
}

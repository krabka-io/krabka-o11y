/// Exact Prometheus `MixedClassicNativeHistogramsWarning` text.
pub(crate) fn mixed_classic_native_warning(_metric: &str) -> String {
    "PromQL warning: vector contains a mix of classic and native histograms".to_string()
}

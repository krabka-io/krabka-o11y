/// Exact Prometheus `MixedFloatsHistogramsWarning` text for `metric`.
pub(crate) fn mixed_floats_histograms_warning(metric: &str) -> String {
    format!("PromQL warning: encountered a mix of histograms and floats for metric name {metric:?}")
}

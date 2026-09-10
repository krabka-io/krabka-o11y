/// Exact Prometheus `NativeHistogramNotCounterWarning` text for `metric`.
pub(crate) fn native_histogram_not_counter_warning(metric: &str) -> String {
    format!("PromQL warning: this native histogram metric is not a counter: {metric:?}")
}

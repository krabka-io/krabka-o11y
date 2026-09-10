/// Exact Prometheus `NativeHistogramNotGaugeWarning` text for `metric`.
pub(crate) fn native_histogram_not_gauge_warning(metric: &str) -> String {
    format!("PromQL warning: this native histogram metric is not a gauge: {metric:?}")
}

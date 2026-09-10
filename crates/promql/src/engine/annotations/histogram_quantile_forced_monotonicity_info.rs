/// Exact Prometheus `HistogramQuantileForcedMonotonicityInfo` text for `metric`.
///
/// Prometheus leaves the metric name off when it does not know it.
pub(crate) fn histogram_quantile_forced_monotonicity_info(metric: &str) -> String {
    let info = "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile)";
    if metric.is_empty() {
        return info.to_string();
    }
    format!("{info} for metric name {metric:?}")
}

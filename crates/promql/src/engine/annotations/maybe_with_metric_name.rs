/// Appends Prometheus' ` for metric name "…"` suffix, where there is a name.
///
/// This is `maybeAddMetricName`, which leaves the annotation alone when the
/// series it came from carries no `__name__`.
pub(crate) fn maybe_with_metric_name(annotation: &str, metric: &str) -> String {
    if metric.is_empty() {
        return annotation.to_string();
    }
    format!("{annotation} for metric name {metric:?}")
}

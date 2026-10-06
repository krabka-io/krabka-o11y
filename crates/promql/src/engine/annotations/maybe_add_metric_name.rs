pub(crate) fn maybe_add_metric_name(message: String, metric: &str) -> String {
    if metric.is_empty() {
        message
    } else {
        format!("{message} for metric name {metric:?}")
    }
}

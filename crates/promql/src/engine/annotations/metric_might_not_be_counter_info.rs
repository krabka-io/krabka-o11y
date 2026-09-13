use krabka_blockstore::Labels;

use super::emit_info;

pub(crate) fn metric_might_not_be_counter_info(metric: &str, metric_type: &str) -> String {
    format!(
        "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got {metric_type:?}: {metric:?}"
    )
}

pub(crate) fn emit_metric_might_not_be_counter_info(labels: &Labels) {
    let metric = labels.get("__name__").unwrap_or("");
    let metric_type = labels.get("__type__").unwrap_or("");
    if !metric.is_empty() && !matches!(metric_type, "counter" | "histogram") {
        emit_info(metric_might_not_be_counter_info(metric, metric_type));
    }
}

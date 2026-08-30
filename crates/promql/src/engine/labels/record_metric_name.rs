use super::*;

/// Records the `__name__` of the first sample for a histogram group key.
///
/// A later mixed-histogram warning names the metric with this value, as
/// Prometheus does.
pub(crate) fn record_metric_name(names: &mut BTreeMap<String, String>, key: &str, labels: &Labels) {
    if let Some(name) = labels.get("__name__") {
        names
            .entry(key.to_string())
            .or_insert_with(|| name.to_string());
    }
}

use super::{Labels, Value, json_object_to_labels};

pub(crate) fn metric_series_labels(series: &Value) -> Option<Labels> {
    series.get("metric").and_then(json_object_to_labels)
}

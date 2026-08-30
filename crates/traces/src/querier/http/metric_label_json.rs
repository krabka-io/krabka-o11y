use super::{json, Value};

/// One TraceQL-metrics label as Tempo's protojson `commonv1.KeyValue`, which is
/// `{"key": k, "value": {"stringValue": v}}`.
///
/// Grafana's Tempo backend parses the `labels` field as a JSON array, so a map
/// object fails to unmarshal with
/// `cannot unmarshal object into Go value of type []json.RawMessage`.
pub(crate) fn metric_label_json(key: &str, value: &str) -> Value {
    json!({ "key": key, "value": { "stringValue": value } })
}

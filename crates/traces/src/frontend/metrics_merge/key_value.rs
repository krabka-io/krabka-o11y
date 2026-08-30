use super::*;

/// One label as Tempo's `commonv1.KeyValue`: `{"key": k, "value": <AnyValue>}`.
///
/// The value, such as `{"stringValue": "api"}`, stays raw JSON. The merge only
/// compares label sets, and never interprets values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: serde_json::Value,
}

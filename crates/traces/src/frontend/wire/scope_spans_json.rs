use super::*;

/// One OTLP `ScopeSpans` group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSpansJson {
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub scope: serde_json::Value,
    #[serde(default)]
    pub spans: Vec<OtlpSpanJson>,
}

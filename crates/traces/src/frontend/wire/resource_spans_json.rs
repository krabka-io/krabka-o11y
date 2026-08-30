use super::{Deserialize, ScopeSpansJson, Serialize};

/// One OTLP `ResourceSpans` group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpansJson {
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub resource: serde_json::Value,
    #[serde(default)]
    pub scope_spans: Vec<ScopeSpansJson>,
}

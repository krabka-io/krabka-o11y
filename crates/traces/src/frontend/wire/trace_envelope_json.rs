use super::{Deserialize, OtlpSpanJson, ResourceSpansJson, ScopeSpansJson, Serialize};

/// The `trace` envelope: the OTLP `resourceSpans` array.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceEnvelopeJson {
    #[serde(default)]
    pub resource_spans: Vec<ResourceSpansJson>,
}

impl TraceEnvelopeJson {
    /// An envelope that holds `spans` under one resource and one scope, both
    /// left unset.
    #[must_use]
    pub fn of_unscoped_spans(spans: Vec<OtlpSpanJson>) -> Self {
        TraceEnvelopeJson {
            resource_spans: vec![ResourceSpansJson {
                resource: serde_json::Value::Null,
                scope_spans: vec![ScopeSpansJson {
                    scope: serde_json::Value::Null,
                    spans,
                }],
            }],
        }
    }
}

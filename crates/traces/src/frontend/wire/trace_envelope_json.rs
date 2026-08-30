use super::{Serialize, Deserialize, ResourceSpansJson};

/// The `trace` envelope: the OTLP `resourceSpans` array.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceEnvelopeJson {
    #[serde(default)]
    pub resource_spans: Vec<ResourceSpansJson>,
}

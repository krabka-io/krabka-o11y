use super::*;

/// A single matched span, with string-encoded nanos and OTLP-KV attributes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanJson {
    #[serde(rename = "spanID")]
    pub span_id: String,
    pub start_time_unix_nano: String,
    /// Nanos, **string-encoded**. This is a Tempo quirk, so the field mirrors
    /// the wire form verbatim rather than holding a `Time`. The projections on
    /// either side of it convert.
    pub duration_nanos: String,
    #[serde(default)]
    pub attributes: Vec<KeyValueJson>,
}

impl From<&SpanRef> for SpanJson {
    fn from(s: &SpanRef) -> Self {
        SpanJson {
            span_id: hex8(&s.span_id),
            start_time_unix_nano: s.start_time_unix_nano.to_string(),
            duration_nanos: s.duration.nanos_i64().to_string(),
            attributes: s
                .attributes
                .iter()
                .map(|(k, v)| KeyValueJson {
                    key: k.clone(),
                    value: AnyValueJson::from(v),
                })
                .collect(),
        }
    }
}

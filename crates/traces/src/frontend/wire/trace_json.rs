use super::{Serialize, Deserialize, Time, SpanSetJson, TraceResult, hex16};

/// One matched trace in the search response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceJson {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(default)]
    pub root_service_name: String,
    #[serde(default)]
    pub root_trace_name: String,
    /// Nanos since epoch, **string-encoded**. This is a Tempo quirk.
    pub start_time_unix_nano: String,
    /// How long the trace ran.
    ///
    /// This renders as `durationMs`, a whole-millisecond integer, which is the
    /// encoding Tempo's search response uses.
    ///
    /// The value is truncated rather than rounded, because Tempo
    /// integer-divides its nanosecond duration. A report of one millisecond
    /// more than Tempo gives for the same span would show up as a diff in the
    /// differential suite.
    #[serde(
        rename = "durationMs",
        default,
        with = "krabka_units::serde_units::numeric::millis_i64_trunc"
    )]
    pub duration: Time,
    #[serde(default)]
    pub span_sets: Vec<SpanSetJson>,
}

impl From<&TraceResult> for TraceJson {
    fn from(t: &TraceResult) -> Self {
        TraceJson {
            trace_id: hex16(&t.trace_id),
            root_service_name: t.root_service_name.clone(),
            root_trace_name: t.root_trace_name.clone(),
            start_time_unix_nano: t.start_time_unix_nano.to_string(),
            duration: t.duration,
            span_sets: t.span_sets.iter().map(SpanSetJson::from).collect(),
        }
    }
}

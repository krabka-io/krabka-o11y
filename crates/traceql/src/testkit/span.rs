use super::*;

/// One fixture span. Unset fields default to span `1` of trace `1`, a root
/// named `span` that lasts no time and carries no attributes.
pub(crate) struct SpanFixture<'a> {
    /// Every byte of the trace id.
    pub(crate) trace: u8,
    /// Every byte of the span id. The span starts at `1_000 + id` ns.
    pub(crate) id: u8,
    /// Every byte of the parent span id, or `None` for a root span.
    pub(crate) parent: Option<u8>,
    pub(crate) name: &'a str,
    pub(crate) duration_nanos: i64,
    pub(crate) attrs: Vec<(&'a str, AttrValue)>,
}

impl Default for SpanFixture<'_> {
    fn default() -> Self {
        Self {
            trace: 1,
            id: 1,
            parent: None,
            name: "span",
            duration_nanos: 0,
            attrs: Vec::new(),
        }
    }
}

impl SpanFixture<'_> {
    /// The span as the store ingests it.
    pub(crate) fn input_span(self) -> InputSpan {
        InputSpan {
            trace_id: [self.trace; 16],
            span_id: [self.id; 8],
            parent_span_id: self.parent.map(|p| [p; 8]),
            name: self.name.into(),
            kind: 0,
            start_unix_nano: 1_000 + i64::from(self.id),
            duration: Time::from_nanos(self.duration_nanos),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            attrs: self
                .attrs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            events: Vec::new(),
            links: Vec::new(),
        }
    }
}

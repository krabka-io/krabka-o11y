// A server span of the `api` service, for the block-building suites.

use krabka_traces::{AttrValue, KeyValue, Span, SpanKind, StatusCode};

/// A server span of the `api` service in `trace_id`, with id and name taken
/// from `span_id` and an optional parent of the same trace.
pub struct FixtureSpan {
    pub trace_id: [u8; 16],
    pub span_id: u8,
    pub parent: Option<u8>,
    pub start_ns: i64,
}

impl FixtureSpan {
    pub fn build(self) -> Span {
        let Self {
            trace_id,
            span_id,
            parent,
            start_ns,
        } = self;
        Span {
            trace_id,
            span_id: [span_id; 8],
            parent_span_id: parent.map(|id| [id; 8]),
            name: format!("span-{span_id}"),
            kind: SpanKind::Server,
            start_ns,
            duration_ns: 5,
            status: StatusCode::Ok,
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: vec![KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("GET".into()),
            }],
            instrumentation_scope: "test".into(),
            ..Span::default()
        }
    }
}

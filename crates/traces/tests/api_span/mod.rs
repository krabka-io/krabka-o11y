// The `api` service's server span that the block-building, compaction and
// live-store suites build their spans on.

use krabka_traces::{AttrValue, KeyValue, Span, SpanKind, StatusCode};

/// A successful `GET` server span of the `api` service, named `span-{span_id}`
/// and scoped `test`.
pub struct ApiSpan {
    pub trace_id: [u8; 16],
    pub span_id: u8,
    pub start_ns: i64,
    pub duration_ns: i64,
}

impl ApiSpan {
    pub fn build(self) -> Span {
        let Self {
            trace_id,
            span_id,
            start_ns,
            duration_ns,
        } = self;
        Span {
            trace_id,
            span_id: [span_id; 8],
            name: format!("span-{span_id}"),
            kind: SpanKind::Server,
            start_ns,
            duration_ns,
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

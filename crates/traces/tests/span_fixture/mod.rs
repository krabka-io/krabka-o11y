// A server span of the `api` service in `trace_id`, with id and name taken
// from `span_id` and an optional parent of the same trace.

use krabka_traces::{AttrValue, KeyValue, Span, SpanKind, StatusCode};

pub fn span(trace_id: [u8; 16], span_id: u8, parent: Option<u8>, start_ns: i64) -> Span {
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

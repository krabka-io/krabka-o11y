//! Span fixtures shared by unit tests.

use super::{AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode};

/// A string-valued attribute.
pub(crate) fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue::new(key, AttrValue::Str(value.into()))
}

/// A successful 100 ns `GET /` server span of service `api`, starting at
/// 1 000 ns, with trace id `[1; 16]` and span id `[2; 8]`.
pub(crate) fn api_server_span() -> Span {
    Span {
        trace_id: [1; 16],
        span_id: [2; 8],
        name: "GET /".into(),
        kind: SpanKind::Server,
        start_ns: 1_000,
        duration_ns: 100,
        status: StatusCode::Ok,
        resource_attrs: vec![string_attr("service.name", "api")],
        ..Span::default()
    }
}

pub(crate) fn server_span_with_event_and_link(
    name: &str,
    duration_ns: i64,
    span_attrs: Vec<KeyValue>,
    event_time_unix_nano: i64,
    event_attrs: Vec<KeyValue>,
) -> Span {
    Span {
        name: name.into(),
        duration_ns,
        span_attrs,
        events: vec![EventRecord {
            time_unix_nano: event_time_unix_nano,
            name: "exception".into(),
            attrs: event_attrs,
        }],
        links: vec![LinkRecord {
            trace_id: [9; 16],
            span_id: [8; 8],
            attrs: vec![string_attr("link.kind", "retry")],
        }],
        instrumentation_scope: "otel-rust".into(),
        instrumentation_version: "1.2.3".into(),
        ..api_server_span()
    }
}

/// One attribute per array shape a block must keep apart: `empty` (no
/// elements), `one` (a singleton), `many` (a homogeneous array), and `mixed`
/// (elements of different types).
pub(crate) fn array_shape_attrs() -> Vec<KeyValue> {
    vec![
        KeyValue::new("empty", AttrValue::Array(Vec::new())),
        KeyValue::new("one", AttrValue::Array(vec![AttrValue::Int(7)])),
        KeyValue::new(
            "many",
            AttrValue::Array(vec![AttrValue::Bool(true), AttrValue::Bool(false)]),
        ),
        KeyValue::new(
            "mixed",
            AttrValue::Array(vec![AttrValue::Int(7), AttrValue::Str("seven".into())]),
        ),
    ]
}

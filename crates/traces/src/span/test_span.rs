//! Span fixtures shared by unit tests.

use super::{AttrValue, KeyValue, Span, SpanKind, StatusCode};

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

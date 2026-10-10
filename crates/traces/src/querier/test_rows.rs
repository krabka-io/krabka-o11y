//! Block span rows shared by the querier's unit tests.

use krabka_blockstore::{NestedSet, SpanKind, SpanRow, StatusCode};
use krabka_units::nanos;

/// A root `otel-rust` server span of service `api`, alone in its trace: it
/// starts at 1 000 ns, runs 500 ns, and carries no name or attributes.
pub(crate) fn api_root_server_row(trace_id: [u8; 16], span_id: [u8; 8]) -> SpanRow {
    SpanRow {
        trace_id,
        span_id,
        parent_span_id: None,
        nested_set: NestedSet {
            nested_set_left: 1,
            nested_set_right: 2,
            parent_id: 0,
        },
        child_count: 0,
        root_service_name: Some("api".into()),
        root_span_name: None,
        trace_start_unix_nano: 1_000,
        trace_duration: nanos(500),
        name: None,
        kind: SpanKind::Server,
        start_unix_nano: 1_000,
        duration: nanos(500),
        status_code: StatusCode::Ok,
        status_message: None,
        instrumentation_name: Some("otel-rust".into()),
        instrumentation_version: None,
        attrs: Vec::new(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

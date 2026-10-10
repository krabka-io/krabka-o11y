// A Jaeger gRPC push for tests: one `GET /grpc` span of service `checkout`.
//
// The distributor's unit tests and the `tenant_isolation` suite share this
// file, the suite through `#[path]`. It reaches the generated types through
// the `api_v2` module its parent puts in scope.

use super::api_v2::{Batch, PostSpansRequest, Process, Span};

/// The `GET /grpc` span: trace `...01...02`, span `...03`, 25 µs long, starting
/// at one second; every other field is the proto default.
pub fn checkout_grpc_span() -> Span {
    Span {
        trace_id: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
        span_id: vec![0, 0, 0, 0, 0, 0, 0, 3],
        operation_name: "GET /grpc".into(),
        start_time: Some(prost_types::Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        duration: Some(prost_types::Duration {
            seconds: 0,
            nanos: 25_000,
        }),
        ..Span::default()
    }
}

/// A push of `span` alone, in a batch whose process is service `checkout`.
pub fn checkout_post_spans_request(span: Span) -> PostSpansRequest {
    PostSpansRequest {
        batch: Some(Batch {
            process: Some(Process {
                service_name: "checkout".into(),
                tags: Vec::new(),
            }),
            spans: vec![span],
        }),
    }
}

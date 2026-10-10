// A metrics-generator span record, the input of the service-graph loop.

use krabka_traces::metricsgen::{
    SpanKind as MetricsSpanKind, SpanRecord as MetricsSpanRecord, StatusCode as MetricsStatusCode,
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

// One `tenant-a` span of trace `0x11..`, named `op`, for `service`.
pub fn metrics_span(
    service: &str,
    span_id: [u8; 8],
    parent: [u8; 8],
    kind: MetricsSpanKind,
    status: MetricsStatusCode,
    duration_ns: i64,
) -> MetricsSpanRecord {
    MetricsSpanRecord {
        tenant: "tenant-a".into(),
        trace_id: [0x11; 16],
        span_id,
        parent_span_id: parent,
        name: "op".into(),
        kind,
        start_ns: 0,
        duration_ns,
        status,
        status_message: String::new(),
        service_name: service.into(),
        attributes: vec![],
        resource_attributes: vec![],
        size: ByteSize::from_bytes(0),
    }
}

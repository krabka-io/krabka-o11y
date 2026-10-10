// A metrics-generator span record, the input of the service-graph loop.

use krabka_traces::metricsgen::{
    SpanKind as MetricsSpanKind, SpanRecord as MetricsSpanRecord, StatusCode as MetricsStatusCode,
};

/// One `tenant-a` span of trace `0x11..`, named `op`, starting at 0.
pub struct MetricsSpan {
    pub service: &'static str,
    pub span_id: [u8; 8],
    pub parent: [u8; 8],
    pub kind: MetricsSpanKind,
    pub status: MetricsStatusCode,
    pub duration_ns: i64,
}

impl MetricsSpan {
    pub fn record(self) -> MetricsSpanRecord {
        MetricsSpanRecord {
            tenant: "tenant-a".into(),
            trace_id: [0x11; 16],
            span_id: self.span_id,
            parent_span_id: self.parent,
            name: "op".into(),
            kind: self.kind,
            duration_ns: self.duration_ns,
            status: self.status,
            service_name: self.service.into(),
            ..MetricsSpanRecord::default()
        }
    }
}

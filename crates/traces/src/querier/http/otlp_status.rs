use super::OtlpStatus;

pub(crate) fn otlp_status(code: i32, message: &str) -> OtlpStatus {
    // Tempo constructs a Status for every span, and Grafana's Tempo backend
    // dereferences `span.Status` when transforming the protobuf trace
    // (trace_transform.go) — an absent/nil status is a nil pointer dereference
    // that 500s the trace view. STATUS_CODE_UNSET (0) is a valid, present status,
    // so this is emitted unconditionally (wrapped in `Some` at the call site).
    OtlpStatus {
        code,
        message: message.to_string(),
    }
}

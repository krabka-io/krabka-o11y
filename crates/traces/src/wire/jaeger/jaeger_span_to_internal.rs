use super::{JaegerSpan, JaegerProcess, Span, KeyValue, AttrValue, i64_bytes, LinkRecord, trace_id, TraceIdHigh, TraceIdLow, ref_type_name, span_kind, span_status, span_logs_to_events};

pub(crate) fn jaeger_span_to_internal(span: &JaegerSpan, process: &JaegerProcess) -> Span {
    let mut resource_attrs = process.tags.clone();
    resource_attrs.push(KeyValue {
        key: "service.name".into(),
        value: AttrValue::Str(process.service_name.clone()),
    });
    let parent_span_id = span
        .references
        .iter()
        .find(|reference| reference.ref_type == 0)
        .map(|reference| i64_bytes(reference.span_id))
        .or_else(|| (span.parent_span_id != 0).then(|| i64_bytes(span.parent_span_id)));
    let links = span
        .references
        .iter()
        .filter(|reference| reference.ref_type != 0)
        .map(|reference| LinkRecord {
            trace_id: trace_id(
                TraceIdHigh(reference.trace_id_high),
                TraceIdLow(reference.trace_id_low),
            ),
            span_id: i64_bytes(reference.span_id),
            attrs: vec![KeyValue {
                key: "ref.type".into(),
                value: AttrValue::Str(ref_type_name(reference.ref_type).into()),
            }],
        })
        .collect();
    Span {
        trace_id: trace_id(
            TraceIdHigh(span.trace_id_high),
            TraceIdLow(span.trace_id_low),
        ),
        span_id: i64_bytes(span.span_id),
        parent_span_id,
        name: span.operation_name.clone(),
        kind: span_kind(&span.tags),
        start_ns: span.start_time_micros.saturating_mul(1_000),
        duration_ns: span.duration_micros.saturating_mul(1_000),
        status: span_status(&span.tags),
        status_message: String::new(),
        resource_attrs,
        span_attrs: span.tags.clone(),
        events: span_logs_to_events(&span.logs),
        links,
        instrumentation_scope: String::new(),
        instrumentation_version: String::new(),
    }
}

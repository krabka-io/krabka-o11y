use super::*;

pub(crate) fn otlp_span(trace_id: [u8; 16], span: &SpanRef) -> OtlpSpan {
    OtlpSpan {
        trace_id: trace_id.to_vec(),
        span_id: span.span_id.to_vec(),
        parent_span_id: span
            .parent_span_id
            .map(|parent| parent.to_vec())
            .unwrap_or_default(),
        name: span.name.clone(),
        kind: span.kind,
        start_time_unix_nano: span.start_time_unix_nano,
        end_time_unix_nano: span_end_unix_nano(span),
        attributes: otlp_attrs(&span_attributes(span)),
        events: span
            .events
            .iter()
            .map(|event| otlp_event(span, event))
            .collect(),
        links: span.links.iter().map(otlp_link).collect(),
        status: Some(otlp_status(span.status_code, &span.status_message)),
        ..OtlpSpan::default()
    }
}

use super::{TraceSpans, OtlpTracesData, resource_span_groups, OtlpResourceSpans, OtlpResource, otlp_attrs, otlp_scope_spans};

pub(crate) fn trace_traces_data(trace: &TraceSpans, max_trace_spans: usize) -> OtlpTracesData {
    OtlpTracesData {
        resource_spans: resource_span_groups(trace, trace.spans.len().min(max_trace_spans))
            .into_iter()
            .map(|(attrs, spans)| OtlpResourceSpans {
                resource: Some(OtlpResource {
                    attributes: otlp_attrs(&attrs),
                    ..OtlpResource::default()
                }),
                scope_spans: otlp_scope_spans(trace.trace_id, spans),
                ..OtlpResourceSpans::default()
            })
            .collect(),
    }
}

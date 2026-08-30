use super::{json, TraceSpans, Value, resource_span_groups, attrs_json, scope_spans_json};

pub(crate) fn trace_json(trace: &TraceSpans, max_trace_spans: usize) -> Value {
    let total_spans = trace.spans.len();
    let returned_spans = total_spans.min(max_trace_spans);
    let status = if returned_spans < total_spans {
        "PARTIAL"
    } else {
        "COMPLETE"
    };
    let message = if returned_spans < total_spans {
        format!("trace truncated after {returned_spans} spans")
    } else {
        String::new()
    };

    json!({
        "trace": {
            "resourceSpans": resource_span_groups(trace, returned_spans)
                .into_iter()
                .map(|(attrs, spans)| {
                    json!({
                        "resource": {
                            "attributes": attrs_json(&attrs),
                        },
                        "scopeSpans": scope_spans_json(trace.trace_id, spans),
                    })
                })
                .collect::<Vec<_>>(),
        },
        "status": status,
        "message": message,
    })
}

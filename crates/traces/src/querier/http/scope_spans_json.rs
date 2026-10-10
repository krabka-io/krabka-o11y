use super::{
    SpanRef, Value, group_by_instrumentation, instrumentation_scope_json, json, trace_span_json,
};

pub(crate) fn scope_spans_json(trace_id: [u8; 16], input_spans: Vec<&SpanRef>) -> Value {
    let groups = group_by_instrumentation(input_spans);
    Value::Array(
        groups
            .into_iter()
            .map(|((name, version, attributes), spans)| {
                json!({
                    "scope": instrumentation_scope_json(&name, &version, &attributes),
                    "spans": spans
                        .into_iter()
                        .map(|span| trace_span_json(trace_id, span))
                        .collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

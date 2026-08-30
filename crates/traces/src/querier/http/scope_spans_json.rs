use super::*;

pub(crate) fn scope_spans_json(trace_id: [u8; 16], input_spans: Vec<&SpanRef>) -> Value {
    let mut groups: InstrumentationGroups<'_> = Vec::new();
    for span in input_spans {
        let key = (
            span.instrumentation_name.clone(),
            span.instrumentation_version.clone(),
            instrumentation_attributes(span),
        );
        if let Some((_, spans)) = groups.iter_mut().find(|(existing, _)| existing == &key) {
            spans.push(span);
        } else {
            groups.push((key, vec![span]));
        }
    }

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

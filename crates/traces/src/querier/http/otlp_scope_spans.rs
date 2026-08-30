use super::*;

pub(crate) fn otlp_scope_spans(trace_id: [u8; 16], input_spans: Vec<&SpanRef>) -> Vec<OtlpScopeSpans> {
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

    groups
        .into_iter()
        .map(|((name, version, attributes), spans)| OtlpScopeSpans {
            scope: (!name.is_empty() || !version.is_empty() || !attributes.is_empty()).then_some(
                InstrumentationScope {
                    name,
                    version,
                    attributes: otlp_attrs(&attributes),
                    ..InstrumentationScope::default()
                },
            ),
            spans: spans
                .into_iter()
                .map(|span| otlp_span(trace_id, span))
                .collect(),
            ..OtlpScopeSpans::default()
        })
        .collect()
}

use super::{
    InstrumentationScope, OtlpScopeSpans, SpanRef, group_by_instrumentation, otlp_attrs, otlp_span,
};

pub(crate) fn otlp_scope_spans(
    trace_id: [u8; 16],
    input_spans: Vec<&SpanRef>,
) -> Vec<OtlpScopeSpans> {
    let groups = group_by_instrumentation(input_spans);
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

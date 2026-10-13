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
        .map(|group| OtlpScopeSpans {
            scope: (!group.key.name.is_empty()
                || !group.key.version.is_empty()
                || !group.key.attributes.is_empty())
            .then_some(InstrumentationScope {
                name: group.key.name,
                version: group.key.version,
                attributes: otlp_attrs(&group.key.attributes),
                ..InstrumentationScope::default()
            }),
            spans: group
                .spans
                .into_iter()
                .map(|span| otlp_span(trace_id, span))
                .collect(),
            ..OtlpScopeSpans::default()
        })
        .collect()
}

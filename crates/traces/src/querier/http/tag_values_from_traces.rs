use super::*;

pub(crate) fn tag_values_from_traces(traces: &[TraceSpans], tag: &str) -> Vec<TypedValue> {
    let tag = tag.strip_prefix('.').unwrap_or(tag);
    let (attr_tag, attr_scope) = scoped_attribute_tag(tag);
    let mut values = BTreeSet::new();
    for trace in traces {
        collect_trace_intrinsic_values(trace, tag, &mut values);
        if matches!(attr_scope, None | Some(TagScope::Resource)) {
            values.extend(
                trace_resource_attributes(trace)
                    .into_iter()
                    .filter(|(key, _)| key == attr_tag)
                    .map(|(_, value)| typed_value_parts(&value)),
            );
        }
        for span in &trace.spans {
            collect_span_intrinsic_values(span, &trace.spans, tag, &mut values);
            collect_event_values(span, tag, &mut values);
            collect_link_values(span, tag, &mut values);
            if matches!(attr_scope, None | Some(TagScope::Span)) {
                values.extend(
                    span.attributes
                        .iter()
                        .filter(|(key, _)| key == attr_tag)
                        .map(|(_, value)| typed_value_parts(value)),
                );
            }
        }
    }
    values
        .into_iter()
        .map(|(type_, value)| TypedValue { type_, value })
        .collect()
}

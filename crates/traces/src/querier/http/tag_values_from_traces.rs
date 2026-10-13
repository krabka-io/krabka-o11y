use krabka_traceql::{collect_event_values, collect_link_values};

use super::{
    BTreeSet, TagScope, TraceSpans, TypedValue, collect_span_intrinsic_values,
    collect_trace_intrinsic_values, trace_resource_attributes, typed_value_parts,
};

pub(crate) fn tag_values_from_traces(traces: &[TraceSpans], tag: &str) -> Vec<TypedValue> {
    let tag = tag.strip_prefix('.').unwrap_or(tag);
    let prefix = TagScope::split_resource_or_span_prefix(tag);
    let attr_tag = prefix.tag;
    let attr_scope = prefix.scope;
    let mut values = BTreeSet::new();
    for trace in traces {
        collect_trace_intrinsic_values(trace, tag, &mut values);
        if matches!(attr_scope, None | Some(TagScope::Resource)) {
            values.extend(
                trace_resource_attributes(trace)
                    .into_iter()
                    .filter(|attribute| attribute.key == attr_tag)
                    .flat_map(|attribute| typed_value_parts(&attribute.value)),
            );
        }
        for span in &trace.spans {
            collect_span_intrinsic_values(span, &trace.spans, tag, &mut values);
            collect_event_values(&span.events, tag, &mut values);
            collect_link_values(&span.links, tag, &mut values);
            if matches!(attr_scope, None | Some(TagScope::Span)) {
                values.extend(
                    span.attributes
                        .iter()
                        .filter(|attribute| attribute.key == attr_tag)
                        .flat_map(|attribute| typed_value_parts(&attribute.value)),
                );
            }
        }
    }
    values.into_iter().collect()
}

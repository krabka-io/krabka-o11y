use super::{ResourceSpanGroup, TraceSpans, span_resource_attributes};

pub(crate) fn resource_span_groups(
    trace: &TraceSpans,
    returned_spans: usize,
) -> Vec<ResourceSpanGroup<'_>> {
    let mut groups: Vec<ResourceSpanGroup<'_>> = Vec::new();
    for span in trace.spans.iter().take(returned_spans) {
        let attrs = span_resource_attributes(trace, span);
        if let Some(group) = groups.iter_mut().find(|group| group.attributes == attrs) {
            group.spans.push(span);
        } else {
            groups.push(ResourceSpanGroup {
                attributes: attrs,
                spans: vec![span],
            });
        }
    }
    groups
}

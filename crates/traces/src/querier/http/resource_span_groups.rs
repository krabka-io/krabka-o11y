use super::*;

pub(crate) fn resource_span_groups(trace: &TraceSpans, returned_spans: usize) -> Vec<ResourceSpanGroup<'_>> {
    let mut groups: Vec<ResourceSpanGroup<'_>> = Vec::new();
    for span in trace.spans.iter().take(returned_spans) {
        let attrs = span_resource_attributes(trace, span);
        if let Some((_, spans)) = groups
            .iter_mut()
            .find(|(existing_attrs, _)| existing_attrs == &attrs)
        {
            spans.push(span);
        } else {
            groups.push((attrs, vec![span]));
        }
    }
    groups
}

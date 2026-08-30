use super::{AttrValue, SpanRef, TraceSpans, dedup_attrs, trace_resource_attributes};

pub(crate) fn span_resource_attributes(
    trace: &TraceSpans,
    span: &SpanRef,
) -> Vec<(String, AttrValue)> {
    if span.resource_attributes.is_empty() {
        trace_resource_attributes(trace)
    } else {
        dedup_attrs(&span.resource_attributes, "")
    }
}

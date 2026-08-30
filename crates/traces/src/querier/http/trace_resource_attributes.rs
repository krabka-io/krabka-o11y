use super::{AttrValue, TraceSpans, dedup_attrs};

pub(crate) fn trace_resource_attributes(trace: &TraceSpans) -> Vec<(String, AttrValue)> {
    dedup_attrs(&trace.resource_attributes, trace.root_service_name.as_str())
}

use super::{TraceSpans, dedup_attrs};

pub(crate) fn trace_resource_attributes(trace: &TraceSpans) -> Vec<krabka_traceql::Attribute> {
    dedup_attrs(&trace.resource_attributes, trace.root_service_name.as_str())
}

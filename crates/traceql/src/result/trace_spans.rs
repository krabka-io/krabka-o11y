use super::{Attribute, SpanRef};

/// Full span set for one trace.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceSpans {
    pub trace_id: [u8; 16],
    pub root_service_name: String,
    pub root_trace_name: String,
    pub resource_attributes: Vec<Attribute>,
    pub spans: Vec<SpanRef>,
}

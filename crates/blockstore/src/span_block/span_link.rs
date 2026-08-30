/// One nested span link.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanLink {
    pub linked_trace_id: [u8; 16],
    pub linked_span_id: [u8; 8],
    pub attrs: Vec<(String, String)>,
}

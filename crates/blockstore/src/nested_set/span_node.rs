use super::*;

/// One span's tree linkage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanNode {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
}

use super::*;

/// One linked span reference attached to a returned span.
#[derive(Clone, Debug, PartialEq)]
pub struct LinkRef {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attributes: Vec<(String, AttrValue)>,
}

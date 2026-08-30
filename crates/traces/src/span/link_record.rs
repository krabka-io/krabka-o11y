use super::{Serialize, Deserialize, KeyValue};

/// A linked span reference.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attrs: Vec<KeyValue>,
}

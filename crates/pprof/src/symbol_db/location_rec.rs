use super::{Deserialize, LineRec, Serialize};

/// A program location and its inlined lines, innermost first.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocationRec {
    pub address: u64,
    pub mapping_id: u32,
    pub lines: Vec<LineRec>,
}

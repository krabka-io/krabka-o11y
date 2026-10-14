use super::{Deserialize, Serialize};

/// A line in a location, with its function reference and source line number.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalLine {
    pub function_id: u32,
    pub line: i64,
}

/// A location: an address plus lines `(function_id, line)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalLocation {
    pub address: u64,
    pub mapping_id: u32,
    pub lines: Vec<WalLine>,
}

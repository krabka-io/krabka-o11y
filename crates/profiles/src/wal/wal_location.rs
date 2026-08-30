use super::*;

/// A location: an address plus lines `(function_id, line)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalLocation {
    pub address: u64,
    pub mapping_id: u32,
    pub lines: Vec<(u32, i64)>,
}

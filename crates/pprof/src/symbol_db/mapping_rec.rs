use super::{Deserialize, MappingSymbolization, Serialize};

/// A binary mapping record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MappingRec {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: u32,
    pub build_id: u32,
    pub symbolization: MappingSymbolization,
}

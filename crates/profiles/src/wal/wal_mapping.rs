use super::{Deserialize, Serialize, WalFlag};

/// A mapping. A false `has_functions` flag marks an unsymbolized mapping.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalMapping {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: u32,
    pub build_id: u32,
    pub has_functions: WalFlag,
    pub has_filenames: WalFlag,
    pub has_line_numbers: WalFlag,
    pub has_inline_frames: WalFlag,
}

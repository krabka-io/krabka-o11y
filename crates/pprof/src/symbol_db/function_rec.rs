use super::{Deserialize, Serialize};

/// A function record.
///
/// The string fields index into the string table of `SymbolDb`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FunctionRec {
    pub name: u32,
    pub system_name: u32,
    pub filename: u32,
    pub start_line: i64,
}

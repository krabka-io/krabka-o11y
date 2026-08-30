use super::*;

/// A function entry; string fields are indices into `WalSymbolSet.strings`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFunction {
    pub name: u32,
    pub system_name: u32,
    pub filename: u32,
    pub start_line: i64,
}

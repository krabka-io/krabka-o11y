use super::{Deserialize, Serialize, WalFunction, WalLocation, WalMapping};

/// The profile's symbol tables, index-encoded in pprof shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSymbolSet {
    pub strings: Vec<String>,
    pub functions: Vec<WalFunction>,
    pub locations: Vec<WalLocation>,
    pub mappings: Vec<WalMapping>,
}

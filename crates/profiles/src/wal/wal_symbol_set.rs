use super::{Deserialize, Serialize, WalFunction, WalLocation, WalMapping};

/// The profile's symbol tables, index-encoded in pprof shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSymbolSet {
    pub strings: Vec<String>,
    pub functions: Vec<WalFunction>,
    pub locations: Vec<WalLocation>,
    pub mappings: Vec<WalMapping>,
}

impl WalSymbolSet {
    /// The tables of a profile with one frame: location 0, without a
    /// mapping, resolves to line 1 of `function`, named by `strings[1]`.
    #[must_use]
    pub fn single_frame(function: String) -> Self {
        Self {
            strings: vec![String::new(), function],
            functions: vec![WalFunction {
                name: 1,
                system_name: 1,
                filename: 0,
                start_line: 0,
            }],
            locations: vec![WalLocation {
                address: 0,
                mapping_id: 0,
                lines: vec![(0, 1)],
            }],
            mappings: Vec::new(),
        }
    }
}

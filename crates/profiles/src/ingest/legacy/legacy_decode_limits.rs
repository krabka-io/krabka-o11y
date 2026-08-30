use super::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LegacyDecodeLimits {
    pub max_nodes: usize,
    pub max_path_bytes: ByteSize,
    pub max_trie_depth: usize,
}

impl Default for LegacyDecodeLimits {
    fn default() -> Self {
        Self {
            max_nodes: 500_000,
            max_path_bytes: krabka_units::mebibytes(64),
            max_trie_depth: 4_096,
        }
    }
}

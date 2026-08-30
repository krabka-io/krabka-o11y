use super::*;

/// One level of the explicit trie work-stack: the accumulated key prefix for a
/// node plus the number of its declared children that the walk must still
/// visit.
pub(crate) struct TrieFrame {
    pub(crate) key: Vec<u8>,
    pub(crate) remaining: usize,
}

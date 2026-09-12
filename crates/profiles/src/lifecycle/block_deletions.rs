use super::{BlockDeletion, symdb_key};

/// Pairs each block key with the sidecar objects that belong to it.
///
/// A profile block has exactly one sidecar, its [`symdb_key`]. Deleting the
/// block alone would leave that object in the bucket forever: the orphan sweep
/// would reclaim it, but only after the block has left the index, and only on
/// a deployment that runs the sweep.
#[must_use]
pub fn block_deletions(block_keys: &[String]) -> Vec<BlockDeletion> {
    block_keys
        .iter()
        .map(|key| BlockDeletion {
            object_key: key.clone(),
            sidecars: vec![symdb_key(key)],
        })
        .collect()
}

use super::{BlockDeletion, ObjectPath};

/// The deletion record for one log block, named by its key under `prefix`.
///
/// A block index records the key relative to the object-store prefix, and a
/// delete needs the whole path, so the two are joined here rather than at each
/// caller. The join is the one
/// [`log_block_object_path`](krabka_blockstore::log_block_object_path) does,
/// which is what makes the path match the object the block was written to.
///
/// A log block has no sidecar. The profiles path writes a symbol database
/// beside its block and the metrics path writes a manifest; the log index sits
/// under a prefix of its own instead.
pub(crate) fn log_block_deletion(prefix: &ObjectPath, object_key: &str) -> BlockDeletion {
    BlockDeletion {
        object_key: object_key
            .split('/')
            .fold(prefix.clone(), ObjectPath::join)
            .to_string(),
        sidecars: Vec::new(),
    }
}

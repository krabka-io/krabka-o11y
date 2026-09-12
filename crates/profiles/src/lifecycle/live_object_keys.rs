use super::{BTreeSet, ProfileIndex, symdb_key};

/// Every object under the block prefix that the index still needs.
///
/// This is what [`reconcile_orphans`] is given, and it is the whole of what
/// that sweep spares: it deletes every other object under the prefix. **A set
/// built from the block keys alone would delete every symbol database in the
/// bucket**, because a `.symdb` is named by nothing but its block and appears
/// in no index. Nothing would report it. Every query would keep answering, and
/// every frame in every answer would lose its name. Each live block therefore
/// contributes two keys here: its own, and [`symdb_key`] of it.
///
/// [`reconcile_orphans`]: krabka_blockstore::reconcile_orphans
#[must_use]
pub fn live_object_keys(index: &ProfileIndex) -> BTreeSet<String> {
    index
        .compaction_candidates()
        .into_iter()
        .flat_map(|candidate| {
            let sidecar = symdb_key(&candidate.object_key);
            [candidate.object_key, sidecar]
        })
        .collect()
}

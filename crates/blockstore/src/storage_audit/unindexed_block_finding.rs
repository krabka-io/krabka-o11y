use super::{
    ListedObject, StorageAuditOptions, StorageFinding, StorageFindingKind, is_older_than_grace,
};

/// The finding for a block that nothing makes live: `orphan` once it is
/// older than the grace window, `pending` before.
pub fn unindexed_block_finding(
    key: &str,
    listed: &ListedObject,
    options: &StorageAuditOptions,
    what: &str,
) -> StorageFinding {
    let kind = if is_older_than_grace(&listed.meta, options.grace, options.now) {
        StorageFindingKind::Orphan
    } else {
        StorageFindingKind::Pending
    };
    StorageFinding::new(
        kind,
        listed.object.signal,
        listed.object.tenant.clone(),
        key,
        format!("no {what} names this block"),
    )
}

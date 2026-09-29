use super::{
    LiveBlockSet, StorageAuditOptions, StorageFinding, StorageFindingKind, StorageInventory,
    StorageSignal, unindexed_block_finding,
};

/// Compares the blocks of `signal` in the store with the blocks its index
/// names.
///
/// A stored block the index does not name is `orphan` or `pending`. A named
/// block the store does not hold is `dangling_index_entry`. `what` names the
/// index in the detail text.
pub fn compare_with_live(
    inventory: &StorageInventory,
    signal: StorageSignal,
    live: &LiveBlockSet,
    options: &StorageAuditOptions,
    what: &str,
) -> Vec<StorageFinding> {
    let mut findings = Vec::new();
    for (key, listed) in inventory.blocks(signal) {
        if !live.contains(key) && live.knows(listed.object.tenant.as_deref()) {
            findings.push(unindexed_block_finding(key, listed, options, what));
        }
    }
    for (key, tenant) in &live.live {
        if !inventory.contains(key) {
            findings.push(StorageFinding::new(
                StorageFindingKind::DanglingIndexEntry,
                signal,
                Some(tenant.clone()),
                key,
                format!("the {what} names this block and the store does not hold it"),
            ));
        }
    }
    findings
}

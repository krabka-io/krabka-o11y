use super::{
    ObjectRole, StorageAuditOptions, StorageFinding, StorageFindingKind, StorageInventory,
    StorageSignal, unindexed_block_finding,
};

/// Checks metrics blocks against their `.index` sidecars.
///
/// A metrics block is live when its `.index` sidecar exists. The compactor
/// writes the sidecar after the block, and retention deletes it before the
/// block, so a block without one is `orphan` or `pending`. A sidecar without
/// its block is `dangling_index_entry`: the querier would plan a scan of a
/// block that is gone.
pub fn audit_metrics_objects(
    inventory: &StorageInventory,
    options: &StorageAuditOptions,
) -> Vec<StorageFinding> {
    let mut findings = Vec::new();
    for (key, listed) in inventory.of_signal(StorageSignal::Metrics) {
        match &listed.object.role {
            ObjectRole::Block(_) => {
                let sidecar = format!("{}.index", key.trim_end_matches(".parquet"));
                if !inventory.contains(&sidecar) {
                    findings.push(unindexed_block_finding(
                        key,
                        listed,
                        options,
                        "`.index` sidecar",
                    ));
                }
            }
            ObjectRole::MetricsIndex { block } if !inventory.contains(block) => {
                findings.push(StorageFinding::new(
                    StorageFindingKind::DanglingIndexEntry,
                    StorageSignal::Metrics,
                    listed.object.tenant.clone(),
                    key,
                    format!("names block `{block}`, which the store does not hold"),
                ));
            }
            _ => {}
        }
    }
    findings
}

use super::{
    Arc, DEFAULT_BLOCK_READ_MAX, LiveBlockSet, MetricsIndexManifest, ObjectRole, ObjectStore,
    StorageAuditError, StorageAuditOptions, StorageFinding, StorageFindingKind, StorageInventory,
    StorageSignal, read_capped_object, unindexed_block_finding,
};

/// Reads every metrics `.index` manifest and checks the metrics blocks
/// against them.
///
/// A metrics block is live when a manifest names it, as the metrics
/// retention pass decides. The compactor writes the manifest after the
/// block, and retention deletes it before the block, so a block that no
/// manifest names is `orphan` or `pending`. A manifest whose block is gone is
/// `dangling_index_entry`: the querier would plan a scan of a block that is
/// gone.
///
/// A manifest that does not decode is `unreadable_manifest`. A manifest that
/// names another index key, block or tenant than its key is
/// `index_mismatch`. Either one makes the tenant of the manifest unknown, so
/// the audit calls none of its blocks an orphan. The block beside the
/// manifest and the block the manifest names both stay live.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when a read fails for a reason
/// other than absence, or a manifest is larger than
/// [`DEFAULT_BLOCK_READ_MAX`].
pub async fn audit_metrics_objects(
    store: &Arc<dyn ObjectStore>,
    inventory: &StorageInventory,
    options: &StorageAuditOptions,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    let mut live = LiveBlockSet::default();
    let mut findings = Vec::new();
    for (key, listed) in inventory.of_signal(StorageSignal::Metrics) {
        let ObjectRole::MetricsIndex { block } = &listed.object.role else {
            continue;
        };
        let tenant = listed.object.tenant.clone();
        let finding = |kind, detail: String| {
            StorageFinding::new(kind, StorageSignal::Metrics, tenant.clone(), key, detail)
        };
        // Removed since the listing: retention does that.
        let Some(bytes) = read_capped_object(store, key, DEFAULT_BLOCK_READ_MAX).await? else {
            continue;
        };
        live.live
            .insert(block.clone(), tenant.clone().unwrap_or_default());
        let problem = match MetricsIndexManifest::decode(&bytes) {
            Ok(manifest) => {
                if !inventory.contains(&manifest.block_key) {
                    findings.push(finding(
                        StorageFindingKind::DanglingIndexEntry,
                        format!(
                            "names block `{}`, which the store does not hold",
                            manifest.block_key
                        ),
                    ));
                }
                let mismatch = manifest.mismatch(key, block, tenant.as_deref());
                live.live.insert(manifest.block_key, manifest.tenant);
                mismatch.map(|detail| finding(StorageFindingKind::IndexMismatch, detail))
            }
            Err(error) => Some(finding(StorageFindingKind::UnreadableManifest, error)),
        };
        if let Some(problem) = problem {
            match &tenant {
                Some(tenant) => {
                    live.unknown_tenants.insert(tenant.clone());
                }
                None => live.all_unknown = true,
            }
            findings.push(problem);
        }
    }
    for (key, listed) in inventory.blocks(StorageSignal::Metrics) {
        if !live.contains(key) && live.knows(listed.object.tenant.as_deref()) {
            findings.push(unindexed_block_finding(
                key,
                listed,
                options,
                "`.index` manifest",
            ));
        }
    }
    Ok(findings)
}

use super::{
    Arc, DEFAULT_INDEX_SNAPSHOT_MAX, LiveBlockSet, ObjectStore, StorageAuditError,
    StorageAuditOptions, StorageFinding, StorageFindingKind, StorageInventory, StorageSignal,
    list_index_snapshot_objects, manifest_finding, read_latest_snapshot_manifest,
    read_shard_payload, shard_payload_content_hash, shard_payload_object_key,
    snapshot_tenant_blocks,
};

/// Reads the snapshot index of traces or profiles and returns the blocks it
/// makes live.
///
/// The newest generation must decode. Each shard payload it names must exist
/// and hash to the content checksum the generation records. A tenant whose
/// payloads fail either check is unknown in the returned set, so the audit
/// calls none of its blocks an orphan.
///
/// A store with blocks and no generation at all is `unreadable_manifest`,
/// not a store of orphans. A wrong index key looks exactly like that, and a
/// repair must not delete every block because of a configuration mistake.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the generation listing
/// fails.
pub async fn audit_snapshot_index(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    key: &str,
    inventory: &StorageInventory,
    options: &StorageAuditOptions,
) -> Result<(LiveBlockSet, Vec<StorageFinding>), StorageAuditError> {
    let label = format!("{signal} index snapshot");
    let latest = list_index_snapshot_objects(store, key)
        .await
        .map_err(|error| StorageAuditError::ObjectStore(error.to_string()))?
        .pop()
        .map_or_else(|| key.to_string(), |meta| meta.location.to_string());
    let manifest =
        match read_latest_snapshot_manifest(store, key, DEFAULT_INDEX_SNAPSHOT_MAX, &label).await {
            Ok(Some(manifest)) => manifest,
            Ok(None) => {
                if inventory.blocks(signal).next().is_none() {
                    return Ok((LiveBlockSet::default(), Vec::new()));
                }
                let finding = StorageFinding::new(
                    StorageFindingKind::UnreadableManifest,
                    signal,
                    None,
                    key,
                    "the store holds blocks and no index generation",
                );
                return Ok((LiveBlockSet::unknown(), vec![finding]));
            }
            Err(error) => {
                let finding = manifest_finding(signal, None, latest, &error.to_string());
                return Ok((LiveBlockSet::unknown(), vec![finding]));
            }
        };

    let mut live = LiveBlockSet::default();
    let mut findings = Vec::new();
    for tenant in manifest.tenants() {
        if !options.covers_tenant(Some(tenant)) {
            continue;
        }
        let mut sound = true;
        for shard in manifest.shards_of(tenant) {
            let payload = shard_payload_object_key(key, tenant, shard.range(), &shard.content);
            let finding = if inventory.contains(&payload) {
                match read_shard_payload(store, &payload, DEFAULT_INDEX_SNAPSHOT_MAX).await {
                    Ok(bytes) => {
                        let actual = shard_payload_content_hash(&bytes);
                        (actual != shard.content).then(|| {
                            StorageFinding::new(
                                StorageFindingKind::ChecksumMismatch,
                                signal,
                                Some(tenant.clone()),
                                &payload,
                                format!("content hashes to {actual}"),
                            )
                        })
                    }
                    Err(error) => Some(manifest_finding(
                        signal,
                        Some(tenant.clone()),
                        &payload,
                        &error.to_string(),
                    )),
                }
            } else {
                Some(StorageFinding::new(
                    StorageFindingKind::DanglingIndexEntry,
                    signal,
                    Some(tenant.clone()),
                    &payload,
                    format!(
                        "generation `{latest}` names this payload and the store does not hold it"
                    ),
                ))
            };
            if let Some(finding) = finding {
                sound = false;
                findings.push(finding);
            }
        }
        if !sound {
            live.unknown_tenants.insert(tenant.clone());
            continue;
        }
        match snapshot_tenant_blocks(store, signal, key, tenant, DEFAULT_INDEX_SNAPSHOT_MAX).await {
            Ok(blocks) => {
                live.live
                    .extend(blocks.into_iter().map(|block| (block, tenant.clone())));
            }
            Err(error) => {
                live.unknown_tenants.insert(tenant.clone());
                findings.push(manifest_finding(
                    signal,
                    Some(tenant.clone()),
                    &latest,
                    &error.to_string(),
                ));
            }
        }
    }
    Ok((live, findings))
}

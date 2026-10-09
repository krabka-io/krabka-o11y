use super::{
    BTreeMap, BTreeSet, LiveBlockSet, LogBlockIndex, LogBlockStoreError, ObjectRole, ObjectStore,
    Path, StorageFinding, StorageFindingKind, StorageInventory, StorageSignal, TimeRange,
    log_tenant_index_manifest_object_path, log_tenant_index_shard_manifest_object_path,
    manifest_finding, read_log_index_manifest_from_object_store,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
};

/// Reads the logs manifests and each shard's newest generation to find live blocks.
///
/// A block is live when the global manifest, its tenant's manifest, or one
/// of its tenant's shard manifests names it. A manifest that does not decode
/// makes its tenant unknown, and the global manifest makes every tenant
/// unknown. A shard catalog that names a shard manifest the store does not
/// hold is `dangling_index_entry`, and it makes its tenant unknown too.
///
/// A tenant with blocks and no manifest of any kind is
/// `unreadable_manifest`, and its tenant is unknown. A store audited at the
/// wrong root looks like that, and a repair must not delete every block
/// because of it.
pub async fn audit_log_manifests(
    store: &dyn ObjectStore,
    inventory: &StorageInventory,
) -> (LiveBlockSet, Vec<StorageFinding>) {
    let root = Path::default();
    let mut live = LiveBlockSet::default();
    let mut findings = Vec::new();
    let listed_shards: BTreeMap<_, _> = inventory
        .of_signal(StorageSignal::Logs)
        .filter_map(
            |(key, listed)| match (&listed.object.role, listed.object.tenant.as_deref()) {
                (ObjectRole::LogShardManifest { start_ns, end_ns }, Some(tenant)) => {
                    Some(((tenant, *start_ns, *end_ns), key))
                }
                _ => None,
            },
        )
        .collect();
    let add = |live: &mut LiveBlockSet, blocks: &LogBlockIndex| {
        for block in blocks.blocks() {
            live.live
                .insert(block.key.object_key(), block.key.tenant.clone());
        }
    };
    for (key, listed) in inventory.of_signal(StorageSignal::Logs) {
        let tenant = listed.object.tenant.clone();
        let read = match (&listed.object.role, tenant.as_deref()) {
            (ObjectRole::LogGlobalManifest, _) => {
                read_log_index_manifest_from_object_store(store, &root).await
            }
            (ObjectRole::LogTenantManifest, Some(tenant)) => {
                read_tenant_log_index_manifest_from_object_store(store, &root, tenant).await
            }
            (ObjectRole::LogShardManifest { start_ns, end_ns }, Some(tenant)) => {
                // Key order matches snapshot selection. Read once per shard and
                // report failures against its newest listed generation.
                if listed_shards.get(&(tenant, *start_ns, *end_ns)) != Some(&key) {
                    continue;
                }
                match TimeRange::new(*start_ns, *end_ns) {
                    Ok(range) => {
                        read_tenant_log_index_shard_from_object_store(store, &root, tenant, range)
                            .await
                    }
                    Err(error) => Err(error),
                }
            }
            (ObjectRole::LogShardCatalog, Some(tenant)) => {
                match read_tenant_log_index_shard_ranges_from_object_store(store, &root, tenant)
                    .await
                {
                    Ok(ranges) => {
                        let missing = missing_shards(&listed_shards, tenant, key, &ranges);
                        // A missing shard can be the only manifest that names
                        // a live block, so no block of the tenant is an orphan.
                        if !missing.is_empty() {
                            live.unknown_tenants.insert(tenant.to_string());
                        }
                        findings.extend(missing);
                        continue;
                    }
                    Err(error) => Err(error),
                }
            }
            _ => continue,
        };
        match read {
            Ok((_, blocks)) => add(&mut live, &blocks),
            // Other manifests can be removed after the listing. A missing
            // shard generation leaves its tenant's live blocks unknown.
            Err(LogBlockStoreError::ObjectStore(object_store::Error::NotFound { .. }))
                if !matches!(&listed.object.role, ObjectRole::LogShardManifest { .. }) => {}
            Err(error) => {
                match &tenant {
                    Some(tenant) => {
                        live.unknown_tenants.insert(tenant.clone());
                    }
                    None => live.all_unknown = true,
                }
                findings.push(manifest_finding(
                    StorageSignal::Logs,
                    tenant,
                    key.as_str(),
                    &error.to_string(),
                ));
            }
        }
    }
    findings.extend(unmanifested_tenants(inventory, &mut live));
    (live, findings)
}

fn unmanifested_tenants(
    inventory: &StorageInventory,
    live: &mut LiveBlockSet,
) -> Vec<StorageFinding> {
    let mut with_blocks = BTreeSet::new();
    let mut with_manifest = BTreeSet::new();
    for (_, listed) in inventory.of_signal(StorageSignal::Logs) {
        match (&listed.object.role, listed.object.tenant.as_deref()) {
            (ObjectRole::LogGlobalManifest, _) => return Vec::new(),
            (ObjectRole::Block(_), Some(tenant)) => {
                with_blocks.insert(tenant);
            }
            (
                ObjectRole::LogTenantManifest
                | ObjectRole::LogShardCatalog
                | ObjectRole::LogShardManifest { .. },
                Some(tenant),
            ) => {
                with_manifest.insert(tenant);
            }
            _ => {}
        }
    }
    let root = Path::default();
    with_blocks
        .difference(&with_manifest)
        .map(|tenant| {
            live.unknown_tenants.insert((*tenant).to_string());
            StorageFinding::new(
                StorageFindingKind::UnreadableManifest,
                StorageSignal::Logs,
                Some((*tenant).to_string()),
                log_tenant_index_manifest_object_path(&root, tenant).to_string(),
                "the store holds blocks of this tenant and no logs manifest",
            )
        })
        .collect()
}

fn missing_shards(
    listed_shards: &BTreeMap<(&str, i64, i64), &String>,
    tenant: &str,
    catalog: &str,
    ranges: &[TimeRange],
) -> Vec<StorageFinding> {
    let root = Path::default();
    ranges
        .iter()
        .filter(|range| !listed_shards.contains_key(&(tenant, range.start_ns, range.end_ns)))
        .map(|range| log_tenant_index_shard_manifest_object_path(&root, tenant, *range).to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|shard| {
            StorageFinding::new(
                StorageFindingKind::DanglingIndexEntry,
                StorageSignal::Logs,
                Some(tenant.to_string()),
                shard,
                format!(
                    "shard catalog `{catalog}` names this shard and the store holds no generation"
                ),
            )
        })
        .collect()
}

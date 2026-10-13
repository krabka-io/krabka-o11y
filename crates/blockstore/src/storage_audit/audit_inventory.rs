use super::{
    Arc, ListedObject, ObjectStore, StorageAuditError, StorageAuditOptions, StorageAuditReport,
    StorageFinding, StorageInventory, StorageSignal, StreamExt, audit_delete_state,
    audit_log_manifests, audit_metrics_objects, audit_profile_symbols, audit_snapshot_index,
    check_block, check_log_frontier, classify_object_key, compare_with_live, find_wal_overlaps,
};

pub struct AuditedInventory {
    pub report: StorageAuditReport,
    pub inventory: StorageInventory,
}

/// Runs [`audit_store`](super::audit_store) and also returns the listing it
/// audited, so a repair can compare an object with what the audit saw.
///
/// # Errors
/// Returns the errors of [`audit_store`](super::audit_store).
pub async fn audit_inventory(
    store: &Arc<dyn ObjectStore>,
    options: &StorageAuditOptions,
) -> Result<AuditedInventory, StorageAuditError> {
    let mut inventory = StorageInventory::default();
    let mut listed = 0_usize;
    let mut unclassified = 0_usize;
    let mut listing = store.list(None);
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        listed += 1;
        let key = meta.location.to_string();
        match classify_object_key(&key, options) {
            Some(object) if options.covers_signal(object.signal) => {
                inventory.objects.insert(key, ListedObject { meta, object });
            }
            Some(_) => {}
            None => unclassified += 1,
        }
    }
    drop(listing);

    let mut findings = Vec::new();
    for signal in StorageSignal::ALL {
        if options.covers_signal(signal) {
            findings.extend(audit_signal(store, signal, &inventory, options).await?);
        }
    }
    findings.retain(|finding| options.covers_tenant(finding.tenant.as_deref()));
    let report = StorageAuditReport::new(options.scope(), listed, unclassified, findings);
    Ok(AuditedInventory { report, inventory })
}

async fn audit_signal(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    inventory: &StorageInventory,
    options: &StorageAuditOptions,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    let mut findings = Vec::new();
    for entry in inventory.blocks(signal) {
        let key = entry.key;
        let listed = entry.listed;
        if options.covers_tenant(listed.object.tenant.as_deref()) {
            findings.extend(check_block(store, key, listed, options.verify_data).await?);
        }
    }
    findings.extend(find_wal_overlaps(inventory, signal));
    match signal {
        StorageSignal::Metrics => {
            findings.extend(audit_metrics_objects(store, inventory, options).await?);
            findings.extend(audit_delete_state(store, inventory).await?);
        }
        StorageSignal::Logs => {
            let manifest_audit = audit_log_manifests(store.as_ref(), inventory).await;
            findings.extend(manifest_audit.findings);
            findings.extend(compare_with_live(
                inventory,
                signal,
                &manifest_audit.live,
                options,
                "logs manifest",
            ));
            findings.extend(check_log_frontier(store, inventory).await?);
        }
        StorageSignal::Traces | StorageSignal::Profiles => {
            let key = if signal == StorageSignal::Traces {
                &options.trace_index_key
            } else {
                &options.profile_index_key
            };
            let snapshot_audit =
                audit_snapshot_index(store, signal, key, inventory, options).await?;
            findings.extend(snapshot_audit.findings);
            findings.extend(compare_with_live(
                inventory,
                signal,
                &snapshot_audit.live,
                options,
                "index snapshot",
            ));
            if signal == StorageSignal::Profiles {
                findings.extend(
                    audit_profile_symbols(store, inventory, &snapshot_audit.live, options).await?,
                );
            }
        }
    }
    Ok(findings)
}

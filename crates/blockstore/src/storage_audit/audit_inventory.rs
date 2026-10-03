use super::{
    Arc, ListedObject, ObjectStore, StorageAuditError, StorageAuditOptions, StorageAuditReport,
    StorageFinding, StorageInventory, StorageSignal, StreamExt, audit_delete_state,
    audit_log_manifests, audit_metrics_objects, audit_profile_symbols, audit_snapshot_index,
    check_block, check_log_frontier, classify_object_key, compare_with_live, find_wal_overlaps,
};

/// Runs [`audit_store`](super::audit_store) and also returns the listing it
/// audited, so a repair can compare an object with what the audit saw.
///
/// # Errors
/// Returns the errors of [`audit_store`](super::audit_store).
pub async fn audit_inventory(
    store: &Arc<dyn ObjectStore>,
    options: &StorageAuditOptions,
) -> Result<(StorageAuditReport, StorageInventory), StorageAuditError> {
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
    Ok((report, inventory))
}

async fn audit_signal(
    store: &Arc<dyn ObjectStore>,
    signal: StorageSignal,
    inventory: &StorageInventory,
    options: &StorageAuditOptions,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    let mut findings = Vec::new();
    for (key, listed) in inventory.blocks(signal) {
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
            let (live, manifest_findings) = audit_log_manifests(store.as_ref(), inventory).await;
            findings.extend(manifest_findings);
            findings.extend(compare_with_live(
                inventory,
                signal,
                &live,
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
            let (live, index_findings) =
                audit_snapshot_index(store, signal, key, inventory, options).await?;
            findings.extend(index_findings);
            findings.extend(compare_with_live(
                inventory,
                signal,
                &live,
                options,
                "index snapshot",
            ));
            if signal == StorageSignal::Profiles {
                findings.extend(audit_profile_symbols(store, inventory, &live, options).await?);
            }
        }
    }
    Ok(findings)
}

use super::{
    Arc, LiveBlockSet, ObjectRole, ObjectStore, StorageAuditError, StorageAuditOptions,
    StorageFinding, StorageFindingKind, StorageInventory, StorageSignal, check_symbol_table,
    is_older_than_grace,
};

/// Checks profile blocks against their `.symdb` symbol tables.
///
/// The block builder and the compactor both write the symbol table after the
/// block and before the index names the block. A live block without one
/// cannot be symbolized: `missing_sidecar`. A symbol table whose block is
/// gone, or whose block is an old orphan, is `orphan_sidecar` once it is old
/// too, and `pending` before.
///
/// With `verify_data`, the audit also decodes the symbol table of each live
/// block in scope. A symbol table that does not decode is `corrupt_sidecar`:
/// the querier cannot symbolize the block. The decode reads the whole
/// object, as `verify_data` does for blocks. A damaged symbol table does not
/// change which blocks are live, so a plain audit and a repair do not need
/// it.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when a read fails for a reason
/// other than absence.
pub async fn audit_profile_symbols(
    store: &Arc<dyn ObjectStore>,
    inventory: &StorageInventory,
    live: &LiveBlockSet,
    options: &StorageAuditOptions,
) -> Result<Vec<StorageFinding>, StorageAuditError> {
    let mut findings = Vec::new();
    let old = |key: &str| {
        inventory
            .meta(key)
            .is_some_and(|meta| is_older_than_grace(meta, options.grace, options.now))
    };
    for (key, listed) in inventory.of_signal(StorageSignal::Profiles) {
        let tenant = listed.object.tenant.clone();
        match &listed.object.role {
            ObjectRole::Symbols { block } => {
                if live.contains(block) {
                    continue;
                }
                let block_present = inventory.contains(block);
                if block_present && !live.knows(tenant.as_deref()) {
                    continue;
                }
                let (kind, detail) = if old(key) && (!block_present || old(block)) {
                    let detail = if block_present {
                        format!("block `{block}` is an orphan")
                    } else {
                        format!("block `{block}` is gone")
                    };
                    (StorageFindingKind::OrphanSidecar, detail)
                } else {
                    (
                        StorageFindingKind::Pending,
                        format!("block `{block}` is not in the index yet"),
                    )
                };
                findings.push(StorageFinding::new(
                    kind,
                    StorageSignal::Profiles,
                    tenant,
                    key,
                    detail,
                ));
            }
            ObjectRole::Block(_) if live.contains(key) => {
                let symbols = format!("{key}.symdb");
                if !inventory.contains(&symbols) {
                    findings.push(StorageFinding::new(
                        StorageFindingKind::MissingSidecar,
                        StorageSignal::Profiles,
                        tenant,
                        key,
                        format!("the index names this block and `{symbols}` is gone"),
                    ));
                } else if options.verify_data
                    && options.covers_tenant(tenant.as_deref())
                    && let Some(detail) = check_symbol_table(store, &symbols).await?
                {
                    findings.push(StorageFinding::new(
                        StorageFindingKind::CorruptSidecar,
                        StorageSignal::Profiles,
                        tenant,
                        symbols,
                        detail,
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(findings)
}

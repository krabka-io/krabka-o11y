use super::{
    LiveBlockSet, ObjectRole, StorageAuditOptions, StorageFinding, StorageFindingKind,
    StorageInventory, StorageSignal, is_older_than_grace,
};

/// Checks profile blocks against their `.symdb` symbol tables.
///
/// The block builder and the compactor both write the symbol table after the
/// block and before the index names the block. A live block without one
/// cannot be symbolized: `missing_sidecar`. A symbol table whose block is
/// gone, or whose block is an old orphan, is `orphan_sidecar` once it is old
/// too, and `pending` before.
pub fn audit_profile_symbols(
    inventory: &StorageInventory,
    live: &LiveBlockSet,
    options: &StorageAuditOptions,
) -> Vec<StorageFinding> {
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
                }
            }
            _ => {}
        }
    }
    findings
}

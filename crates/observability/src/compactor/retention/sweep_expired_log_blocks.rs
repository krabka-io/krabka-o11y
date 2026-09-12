use super::{
    BlockDeletion, CompactorRunError, ObjectPath, ObjectStore, RetentionWindows, delete_blocks,
    list_log_index_tenants, sweep_expired_tenant_log_blocks,
};

/// Retires every log block that has fallen outside its tenant's retention
/// window, then deletes the objects.
///
/// The pass covers every tenant the store holds, not the tenants the overrides
/// file names. A tenant with no entry of its own still reads a window from the
/// defaults, so a pass over the configured tenants alone would leave the rest
/// on no window at all.
///
/// Every index of a tenant is rewritten before any of that tenant's objects is
/// deleted. A reader that lists between the two steps never learns of the
/// block, so it never asks for the object.
///
/// An object that will not delete is logged and left. The index no longer
/// names it, so it is an orphan from here on, and the next pass does not
/// reach it.
///
/// # Errors
/// Returns the block-store error when the store cannot be listed, or when an
/// index cannot be read or written. A failed delete is not an error.
pub(crate) async fn sweep_expired_log_blocks(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    now_ns: i64,
    windows: &dyn RetentionWindows,
) -> Result<(), CompactorRunError> {
    let mut deletions: Vec<BlockDeletion> = Vec::new();
    for tenant in list_log_index_tenants(store, prefix).await? {
        deletions.extend(
            sweep_expired_tenant_log_blocks(store, prefix, &tenant, now_ns, windows).await?,
        );
    }
    if deletions.is_empty() {
        return Ok(());
    }

    let report = delete_blocks(store, &deletions).await;
    // Every object the store refused, one line each. A pass that reported only
    // its totals would hide an object that fails on every pass, and that
    // object is the one an operator has to act on.
    for failure in &report.failures {
        tracing::warn!(
            object = %failure.failed_key,
            error = %failure.error,
            "log retention sweep could not delete a block object"
        );
    }
    tracing::info!(
        blocks_deleted = report.blocks_deleted,
        objects_absent = report.objects_absent,
        failed = report.failures.len(),
        "log retention sweep deleted expired blocks"
    );
    Ok(())
}

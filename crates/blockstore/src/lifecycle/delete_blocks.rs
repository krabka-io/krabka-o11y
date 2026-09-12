use super::{
    BlockDeletion, BlockDeletionFailure, BlockDeletionReport, ObjectStore, ObjectStoreExt as _,
    Path, instrument,
};

/// Deletes each block and every sidecar that belongs to it.
///
/// One object that will not delete does not end the pass. A retention sweep
/// covers many tenants, and a single object that a backend refuses would
/// otherwise stop every block behind it in the list from ever being deleted.
/// Each failure is reported against its block in
/// [`BlockDeletionReport::failures`] and the pass carries on, so a caller can
/// log what failed and still credit what worked.
///
/// An object that is already gone counts as success. A sweep that ran before
/// and was interrupted, and a sweep that runs twice, both reach objects that
/// no longer exist, and neither is a fault.
///
/// The deletes are issued one at a time rather than through
/// [`ObjectStore::delete_stream`]. The stream reports a failure without
/// naming the object that caused it, which is exactly what a per-block report
/// needs. [`reconcile_orphans`](super::reconcile_orphans) has no such
/// attribution to keep and does use the stream.
#[instrument(
    level = "debug",
    skip_all,
    fields(
        blocks = deletions.len(),
        deleted = tracing::field::Empty,
        failed = tracing::field::Empty,
    )
)]
pub async fn delete_blocks(
    store: &dyn ObjectStore,
    deletions: &[BlockDeletion],
) -> BlockDeletionReport {
    let mut report = BlockDeletionReport::default();
    for deletion in deletions {
        let block = std::iter::once((&deletion.object_key, true));
        let sidecars = deletion.sidecars.iter().map(|sidecar| (sidecar, false));
        for (key, is_block) in block.chain(sidecars) {
            match store.delete(&Path::from(key.as_str())).await {
                Ok(()) if is_block => report.blocks_deleted += 1,
                Ok(()) => report.sidecars_deleted += 1,
                Err(object_store::Error::NotFound { .. }) => report.objects_absent += 1,
                Err(error) => report.failures.push(BlockDeletionFailure {
                    object_key: deletion.object_key.clone(),
                    failed_key: key.clone(),
                    error: error.to_string(),
                }),
            }
        }
    }

    tracing::Span::current().record("deleted", report.blocks_deleted + report.sidecars_deleted);
    tracing::Span::current().record("failed", report.failures.len());
    report
}

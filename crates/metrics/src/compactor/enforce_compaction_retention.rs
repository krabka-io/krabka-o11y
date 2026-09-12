use super::{
    Arc, BTreeSet, BlockDeletion, BlockTimestampUnit, COMPACTION_OBJECT_PREFIX,
    CompactionCandidate, CompactionIndexManifest, CompactionRetentionError,
    CompactionRetentionStats, DEFAULT_BLOCK_SWEEP_GRACE, ObjectStore, RetentionWindows, SystemTime,
    UNIX_EPOCH, delete_blocks, list_compaction_manifests, plan_expired_blocks, reconcile_orphans,
};

/// Retires the index entries of the metric blocks that fall outside their
/// tenant's retention window, deletes those blocks, then deletes the objects
/// that no manifest names.
///
/// Metrics holds no index in memory. The `.index` manifests beside the blocks
/// are the index, so one pass reads them all: what they say decides which
/// blocks expire, and what they name is the live set the orphan sweep keeps.
///
/// # Index retirement comes first
///
/// Deleting a manifest *is* the index retirement here. It is what the other
/// signals do with `remove_*_blocks` and a snapshot save, and the metrics pass
/// does it in the same place: before any block object is deleted. The order
/// decides what a torn pass leaves behind. Manifest first leaves an
/// unreferenced block object, which the orphan sweep reclaims and which no
/// query ever resolves. Block first leaves a surviving manifest that names an
/// object that is gone, and every query over that window then resolves a key
/// with nothing behind it.
///
/// A block whose manifest would not delete is left alone for the same reason.
/// Its index entry is still live, so deleting the object would produce exactly
/// the state the ordering exists to prevent.
///
/// One object that will not delete does not end the pass.
/// [`CompactionRetentionStats::failures`] carries every such failure for the
/// caller to log, and the next pass reaches the object again.
///
/// # Errors
/// Returns an error when the manifests cannot be read, or when the orphan
/// reconciliation cannot list the prefix.
pub async fn enforce_compaction_retention(
    store: &Arc<dyn ObjectStore>,
    now: SystemTime,
    windows: &dyn RetentionWindows,
) -> Result<CompactionRetentionStats, CompactionRetentionError> {
    // One clock drives both halves. The blocks count epoch milliseconds and
    // the orphan grace counts wall-clock time, and a pass that read two
    // clocks could expire against one and spare against the other. A clock
    // before the epoch, or past the millisecond range of an `i64`, expires
    // nothing rather than everything.
    let now_ms = now
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(i64::MIN);

    let manifests = list_compaction_manifests(store).await?;

    // Both objects of every block, and not the blocks alone. A live set that
    // named only the blocks would leave every manifest unnamed, and the
    // orphan sweep would then delete the index itself: every block still
    // there, and no query able to find one.
    let live_keys: BTreeSet<String> = manifests
        .iter()
        .flat_map(|manifest| [manifest.block_key.clone(), manifest.index_key.clone()])
        .collect();

    let candidates: Vec<CompactionCandidate> = manifests
        .iter()
        .map(|manifest| CompactionCandidate {
            tenant: manifest.tenant.clone(),
            object_key: manifest.block_key.clone(),
            min_ts: manifest.min_ts,
            max_ts: manifest.max_ts,
            row_count: manifest.row_count,
            level: manifest.level,
        })
        .collect();
    let expired_keys: BTreeSet<String> =
        plan_expired_blocks(&candidates, now_ms, BlockTimestampUnit::Millis, windows)
            .into_iter()
            .map(|block| block.object_key)
            .collect();
    let expired: Vec<&CompactionIndexManifest> = manifests
        .iter()
        .filter(|manifest| expired_keys.contains(&manifest.block_key))
        .collect();

    let manifests_retired = delete_blocks(
        store,
        &deletions_of(expired.iter().copied(), |manifest| &manifest.index_key),
    )
    .await;
    // A manifest that would not delete keeps its block. Anything else would
    // leave a live index entry naming an object that is gone.
    let unretired: BTreeSet<&str> = manifests_retired
        .failures
        .iter()
        .map(|failure| failure.failed_key.as_str())
        .collect();
    let retired = expired
        .iter()
        .copied()
        .filter(|manifest| !unretired.contains(manifest.index_key.as_str()));
    let blocks_deleted = delete_blocks(
        store,
        &deletions_of(retired, |manifest| &manifest.block_key),
    )
    .await;

    // The live set still names the expired blocks, so an object that would
    // not delete stays with the retention half of the pass. The orphan sweep
    // does not take it over, and the next pass reports it again.
    let orphans = reconcile_orphans(
        store,
        COMPACTION_OBJECT_PREFIX,
        &live_keys,
        DEFAULT_BLOCK_SWEEP_GRACE,
        now,
    )
    .await?;

    Ok(CompactionRetentionStats {
        manifests_scanned: manifests.len(),
        manifests_retired: manifests_retired.into(),
        blocks_deleted: blocks_deleted.into(),
        orphans,
    })
}

/// One deletion per manifest, naming the object `key_of` picks and no sidecar.
///
/// The sidecar list is what [`delete_blocks`] would use to order a block
/// against its manifest, and it orders the block first. Metrics needs the
/// other order, so each phase names one object and the phases carry the order.
fn deletions_of<'a>(
    manifests: impl Iterator<Item = &'a CompactionIndexManifest>,
    key_of: impl Fn(&'a CompactionIndexManifest) -> &'a String,
) -> Vec<BlockDeletion> {
    manifests
        .map(|manifest| BlockDeletion {
            object_key: key_of(manifest).clone(),
            sidecars: Vec::new(),
        })
        .collect()
}

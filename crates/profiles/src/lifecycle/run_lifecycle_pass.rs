use super::{
    Arc, BTreeMap, BlockTimestampUnit, LifecycleOptions, LifecycleReport, ObjectStore,
    ProfileIndex, ProfilesError, UNIX_EPOCH, block_deletions, compact_once_with_policy,
    delete_blocks, plan_expired_blocks, sweep_orphan_blocks,
};

/// Runs one whole compactor pass over `index`: merge, expire, delete,
/// reconcile.
///
/// # Order
///
/// The order of the four steps is the contract, not an implementation detail.
///
/// 1. Merge. The outputs are written and the inputs leave the index. Their
///    objects stay.
/// 2. Expire. Blocks outside their tenant's retention window leave the index.
///    Their objects stay.
/// 3. Save the index, once, for both. **This is the only durability point.**
///    Until it lands, every block this pass dropped is still live to a reader
///    that loads the published snapshot, and every object it names must still
///    exist.
/// 4. Delete the objects of everything dropped in steps 1 and 2, each block
///    with its symbol database, and then sweep the orphans.
///
/// Deleting before the save would let a querier resolve a block key out of the
/// published index and read a `404` from object storage. Saving before the
/// delete only risks the reverse, an object no index names, and the orphan
/// sweep in step 4 is what reclaims those.
///
/// A pass that changed nothing saves nothing. The save burns a snapshot
/// generation every tick and evicts the retained history a reader falls back
/// on.
///
/// # Errors
/// Returns an error when a merge fails, or when the index snapshot cannot be
/// saved. A single object that will not delete is not one of them: it is
/// counted in the report and the next pass reaches it again.
pub async fn run_lifecycle_pass(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    options: &LifecycleOptions<'_>,
) -> Result<LifecycleReport, ProfilesError> {
    let compaction =
        compact_once_with_policy(store, index, options.policy, options.downsample).await?;
    let mut dropped_keys = compaction.retired_keys;

    // Epoch milliseconds, because that is what a profile block's `min_ts` and
    // `max_ts` count in. A clock the pass cannot read expires nothing, which
    // is the safe direction: the next pass reads it again.
    let now_ms = options
        .now
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_millis()).ok())
        .unwrap_or(0);
    let expired = plan_expired_blocks(
        &index.compaction_candidates(),
        now_ms,
        BlockTimestampUnit::Millis,
        options.retention,
    );
    // Grouped, because a removal is pinned to the tenant that holds the block:
    // one call per tenant is what records the pending removals that keep a
    // snapshot merge from putting the block back.
    let mut by_tenant: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for block in &expired {
        by_tenant
            .entry(block.tenant.clone())
            .or_default()
            .push(block.object_key.clone());
    }
    for (tenant, keys) in &by_tenant {
        index.remove_profile_blocks(tenant, keys);
        dropped_keys.extend(keys.iter().cloned());
    }

    let mut report = LifecycleReport {
        compacted: compaction.outputs,
        expired: expired.len(),
        ..LifecycleReport::default()
    };
    if report.changed_the_index() {
        index
            .save_latest_snapshot_with_retain(
                store,
                options.index_key,
                options.index_snapshot_retain,
            )
            .await
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
    }
    report.deletions = delete_blocks(store, &block_deletions(&dropped_keys)).await;
    report.orphans = sweep_orphan_blocks(
        store,
        index,
        options.block_prefix,
        options.orphan_grace,
        options.now,
    )
    .await?;
    Ok(report)
}

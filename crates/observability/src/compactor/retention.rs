//!
//! Compaction decides which WAL records become a block. This module decides
//! which blocks stop being live, and it deletes them. The expiry rule itself
//! is [`krabka_blockstore::plan_expired_blocks`], the one every signal uses,
//! so logs adapts its own index records into
//! [`krabka_blockstore::CompactionCandidate`] rather than holding a second
//! copy of the rule.
//!
//! The sweep runs inside the compactor loop. Shard changes use conditional
//! writes to retain blocks published by other builders after the sweep read
//! its indexes. Legacy full manifests remain whole-object replacements.
//!
//! The index comes first and the objects come second. A reader that lists
//! after the rewrite never learns of the block, so it never asks for the
//! object. The reverse order leaves the index naming an object that is
//! already deleted.

use krabka_blockstore::LogIndexes;
use crate::TenantLogIndexes;

use krabka_blockstore::{
    BlockDeletion, BlockLevel, BlockTimestampUnit, CompactionCandidate, RetentionWindows,
    delete_blocks, list_tenant_log_index_shard_ranges_from_object_store, plan_expired_blocks,
    unescape_object_path_segment, update_tenant_log_index_shard_to_object_store,
};

use crate::{
    compaction_metrics::CompactionMetrics,
    BTreeMap, BTreeSet, BlockDescriptor, BlockIndex, BlockStoreError, CompactorRunError,
    LabelIndex, ObjectPath, ObjectStore, TimeRange, insert_descriptor_labels,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_or_empty_from_object_store,
    write_tenant_log_index_manifest_to_object_store,
};



/// The deletion record for one log block, named by its key under `prefix`.
///
/// A block index records the key relative to the object-store prefix, and a
/// delete needs the whole path, so the two are joined here rather than at each
/// caller. The join is the one
/// [`log_block_object_path`](krabka_blockstore::log_block_object_path) does,
/// which is what makes the path match the object the block was written to.
///
/// A log block has no sidecar. The profiles path writes a symbol database
/// beside its block and the metrics path writes a manifest; the log index sits
/// under a prefix of its own instead.
pub(crate) fn log_block_deletion(prefix: &ObjectPath, object_key: &str) -> BlockDeletion {
    BlockDeletion {
        object_key: object_key
            .split('/')
            .fold(prefix.clone(), ObjectPath::join)
            .to_string(),
        sidecars: Vec::new(),
    }
}

/// Every tenant that has written anything under `prefix`.
///
/// There is no API that lists the tenants of a deployment, and the overrides
/// file names only the tenants an operator configured. A tenant on the default
/// window has no entry there and still has blocks, so the sweep reads the
/// tenants out of the object store instead: one delimited listing of `prefix`
/// gives a `tenant=<escaped>` prefix for each one.
///
/// A prefix whose escape does not decode is skipped. Nothing Krabka writes
/// produces one, so such a prefix was written by something else and the sweep
/// has no index for it to rewrite.
///
/// # Errors
/// Returns the object store's error when `prefix` cannot be listed.
pub(crate) async fn list_log_index_tenants(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<BTreeSet<String>, CompactorRunError> {
    let listing = store
        .list_with_delimiter(Some(prefix))
        .await
        .map_err(|error| CompactorRunError::BlockStore(BlockStoreError::ObjectStore(error)))?;
    Ok(listing
        .common_prefixes
        .iter()
        .filter_map(|common_prefix| common_prefix.filename())
        .filter_map(|segment| segment.strip_prefix("tenant="))
        .filter_map(unescape_object_path_segment)
        .collect())
}

/// Reduces log block descriptors to the records the shared expiry rule reads.
///
/// [`plan_expired_blocks`](krabka_blockstore::plan_expired_blocks) takes the
/// same record every signal offers its compaction planner, so logs answers in
/// that shape rather than holding an expiry rule of its own.
///
/// `max_ts` is [`BlockKey::time_range`](krabka_blockstore::BlockKey)'s
/// `end_ns`, in epoch nanoseconds. The compactor folds it from the event
/// timestamps of the records before it applies any delete filter, and a block
/// rewrite keeps the key it had, so the bound only ever over-estimates how new
/// a block's newest row is. An over-estimate keeps a block one sweep longer
/// than it had to be, which is the safe direction.
///
/// The row count is `0`, because the log index does not record one. Expiry
/// reads `max_ts` alone, and the level says what a compaction may merge rather
/// than what a sweep may delete.
pub(crate) fn log_retention_candidates<'a>(
    blocks: impl IntoIterator<Item = &'a BlockDescriptor>,
) -> Vec<CompactionCandidate> {
    blocks
        .into_iter()
        .map(|block| CompactionCandidate {
            tenant: block.key.tenant.clone(),
            object_key: block.key.object_key(),
            min_ts: block.key.time_range.start_ns,
            max_ts: block.key.time_range.end_ns,
            row_count: 0,
            level: BlockLevel::INGESTED,
        })
        .collect()
}

/// Rebuilds one index pair without the blocks in `expired`.
///
/// Returns `None` when the index names none of them, so the caller writes
/// nothing. Every log index write is a whole-object put, and a put that only
/// rewrites the bytes that are already there still races another writer.
///
/// The label index is rebuilt from the blocks that stay, not copied. A series
/// that only the retired blocks held has no rows left to match, and leaving it
/// in the postings would answer `/loki/api/v1/series` with a stream that no
/// block holds.
///
/// # Errors
/// [`CompactorRunError::MissingSeriesLabels`] when a block that stays names a
/// fingerprint that the label index does not hold.
pub(crate) fn retire_blocks_from_index(
    tenant: &str,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    expired: &BTreeSet<String>,
) -> Result<Option<TenantLogIndexes>, CompactorRunError> {
    if !block_index
        .blocks()
        .iter()
        .any(|block| expired.contains(&block.key.object_key()))
    {
        return Ok(None);
    }

    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    for block in block_index.blocks() {
        if expired.contains(&block.key.object_key()) {
            continue;
        }
        insert_descriptor_labels(&mut next_label_index, label_index, tenant, block)?;
        next_block_index.insert(block.clone());
    }
    Ok(Some(TenantLogIndexes {
        label_index: next_label_index,
        block_index: next_block_index,
    }))
}

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
    metrics: &CompactionMetrics,
) -> Result<(), CompactorRunError> {
    let mut deletions: Vec<BlockDeletion> = Vec::new();
    for tenant in list_log_index_tenants(store, prefix).await? {
        deletions.extend(
            sweep_expired_tenant_log_blocks(store, prefix, &tenant, now_ns, windows).await?,
        );
    }
    if deletions.is_empty() {
        metrics.record_deleted(0, 0, 0);
        return Ok(());
    }

    let report = delete_blocks(store, &deletions).await;
    metrics.record_deleted(
        report.blocks_deleted as u64,
        report.sidecars_deleted as u64,
        report.failures.len() as u64,
    );
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

struct ShardLogIndexes {
    range: TimeRange,
    indexes: LogIndexes,
}

/// Drops one tenant's expired blocks from every index that names them, and
/// names the objects the caller has to delete.
///
/// The index is rewritten here and the objects are deleted by the caller,
/// after every index of the tenant is written. A reader that lists between the
/// two steps sees an index that does not name the block, so it never asks for
/// the object.
///
/// Empty shards retain an empty manifest. Deleting their generation keys can
/// remove a concurrent append. Pruning keeps the newest eight generations per
/// range; empty ranges continue to accumulate.
///
/// # Errors
/// Returns the block-store error when an index cannot be read or written.
/// An index a tenant has never had is not a fault.
pub(crate) async fn sweep_expired_tenant_log_blocks(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    now_ns: i64,
    windows: &dyn RetentionWindows,
) -> Result<Vec<BlockDeletion>, CompactorRunError> {
    let manifest =
        match read_tenant_log_index_manifest_from_object_store(store, prefix, tenant).await {
            Ok(indexes) => Some(indexes),
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => None,
            Err(error) => return Err(error.into()),
        };
    let mut shards = Vec::new();
    for shard_range in tenant_log_index_shard_ranges(store, prefix, tenant).await? {
        let indexes =
            read_tenant_log_index_shard_from_object_store(store, prefix, tenant, shard_range)
                .await?;
        shards.push(ShardLogIndexes {
            range: shard_range,
            indexes,
        });
    }

    // One descriptor per object key, so a block that both the tenant manifest
    // and a shard manifest names is offered to the expiry rule once.
    let mut blocks: BTreeMap<String, &BlockDescriptor> = BTreeMap::new();
    let shard_blocks = shards
        .iter()
        .flat_map(|shard| shard.indexes.blocks.blocks());
    for block in manifest
        .iter()
        .flat_map(|indexes| indexes.blocks.blocks())
        .chain(shard_blocks)
    {
        blocks.entry(block.key.object_key()).or_insert(block);
    }
    let candidates = log_retention_candidates(blocks.values().copied());
    let expired: BTreeSet<String> =
        plan_expired_blocks(&candidates, now_ns, BlockTimestampUnit::Nanos, windows)
            .into_iter()
            .map(|block| block.object_key)
            .collect();
    if expired.is_empty() {
        return Ok(Vec::new());
    }

    if let Some(indexes) = &manifest
        && let Some(next_indexes) =
            retire_blocks_from_index(tenant, &indexes.labels, &indexes.blocks, &expired)?
    {
        write_tenant_log_index_manifest_to_object_store(
            store,
            prefix,
            tenant,
            &next_indexes.label_index,
            &next_indexes.block_index,
        )
        .await?;
    }

    for shard in &shards {
        let shard_range = shard.range;
        let indexes = &shard.indexes;
        let Some(next_indexes) =
            retire_blocks_from_index(tenant, &indexes.labels, &indexes.blocks, &expired)?
        else {
            continue;
        };
        update_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            shard_range,
            &indexes.blocks,
            &next_indexes.label_index,
            &next_indexes.block_index,
        )
        .await?;
    }

    Ok(expired
        .iter()
        .map(|object_key| log_block_deletion(prefix, object_key))
        .collect())
}

/// Every shard of `tenant` that the sweep has to rewrite.
///
/// The catalog and the shard listing can disagree, and the sweep needs the
/// union of the two. A shard manifest that the catalog does not name is still
/// found by the querier, which lists the shard prefix first and reads the
/// catalog only when that listing is empty. A sweep that read the catalog
/// alone would delete the blocks of such a shard and leave the manifest naming
/// them.
///
/// # Errors
/// Returns the block-store error for every failure except an absent catalog.
/// A tenant that has never had a catalog written is not a fault.
pub(crate) async fn tenant_log_index_shard_ranges(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, CompactorRunError> {
    let catalogued =
        read_tenant_log_index_shard_ranges_or_empty_from_object_store(store, prefix, tenant)
            .await?;
    let listed =
        list_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await?;

    // `TimeRange` is not `Ord`, so the duplicates go after the sort rather
    // than through a set.
    let mut shard_ranges: Vec<TimeRange> = catalogued.into_iter().chain(listed).collect();
    shard_ranges.sort_by(|left, right| {
        left.start_ns
            .cmp(&right.start_ns)
            .then_with(|| left.end_ns.cmp(&right.end_ns))
    });
    shard_ranges.dedup();
    Ok(shard_ranges)
}

use super::{
    Arc, BTreeMap, BTreeSet, BlockDeletion, BlockWriter, ByteSize, CompactionIndexManifest,
    CompactionIndexSink, CompactionPolicy, DeferredBlockDeletions, MetricCompactionError,
    MetricCompactionPass, ObjectStore, delete_blocks, list_compaction_manifests,
    materialize_metric_erasure_requests, merge_metric_blocks, plan_metric_compactions,
};

/// Runs one level-compaction pass over every metric block in object storage.
///
/// A pass plans and applies; it does not loop. Running it again picks up where
/// this one left off, one rung further up the ladder, and eventually plans
/// nothing. Which blocks meet, and when the climbing stops, is the policy's
/// business. The manifests are read afresh every pass, because a block builder
/// publishes new blocks into the same prefix and a stale set would plan against
/// blocks a previous pass replaced.
///
/// # The apply order
///
/// Metrics has no durable index to swap. Its index is the set of `.index`
/// manifests, so applying a merge is four steps in this order:
///
/// 1. Write the merged block.
/// 2. Write its manifest.
/// 3. Delete the inputs' manifests.
/// 4. Delete the inputs' block objects, in a **later** pass.
///
/// Output before inputs. The other way round leaves a window in which a
/// listing finds neither the inputs nor the output, and the samples are simply
/// absent. This way leaves a window of overlapping blocks. Float and histogram
/// queries deduplicate that overlap; the other kinds preserve every input row
/// until the source manifests retire.
///
/// Manifests before blocks, for the reason
/// [`enforce_compaction_retention`](super::enforce_compaction_retention) gives:
/// a surviving manifest that names a deleted object breaks every query over its
/// window, while an unreferenced object breaks nothing.
///
/// And the blocks wait for a later pass, because a querier can hold a cached
/// cold index that still names an input. `deferred` carries them between
/// passes; see [`DeferredBlockDeletions`].
///
/// # Errors
/// Returns an error when the manifests cannot be read, or when a merge cannot
/// read its inputs or write its output. A deletion that the object store
/// refuses is not an error: it is reported in
/// [`MetricCompactionPass::manifests_retired`] or
/// [`MetricCompactionPass::blocks_deleted`], and the orphan sweep reaches the
/// object later.
pub async fn compact_metric_blocks_once<S>(
    store: &Arc<dyn ObjectStore>,
    block_writer: &BlockWriter,
    index_sink: &S,
    policy: CompactionPolicy,
    block_read_max: ByteSize,
    deferred: &mut DeferredBlockDeletions,
) -> Result<MetricCompactionPass, MetricCompactionError>
where
    S: CompactionIndexSink + ?Sized,
{
    let (erasure, retired) =
        materialize_metric_erasure_requests(store, block_writer, index_sink, block_read_max)
            .await?;
    if !erasure.is_empty() {
        deferred.extend(retired);
        return Ok(erasure);
    }

    let manifests = list_compaction_manifests(store).await?;
    let by_key: BTreeMap<&str, &CompactionIndexManifest> = manifests
        .iter()
        .map(|manifest| (manifest.block_key.as_str(), manifest))
        .collect();

    let mut pass = MetricCompactionPass::default();
    let mut retired = Vec::new();
    // A job that fails ends the pass and leaves the jobs before it applied. The
    // output key is a function of the input keys and their object versions, so
    // the next pass replans the same job and writes the same key: the retry
    // overwrites rather than creating another output object. The source
    // manifests remain available for the next pass to retire.
    for planned in plan_metric_compactions(&manifests, policy) {
        let output = merge_metric_blocks(
            store,
            block_writer,
            index_sink,
            &by_key,
            &planned,
            block_read_max,
        )
        .await?;
        retired.extend(planned.job.input_keys.iter().filter_map(|key| {
            by_key
                .get(key.as_str())
                .map(|manifest| (key.clone(), manifest.index_key.clone()))
        }));
        pass.outputs.push(output);
    }

    // Step 3. The manifest is the index entry, so this is where the input
    // leaves the index.
    let index_deletions: Vec<BlockDeletion> = retired
        .iter()
        .map(|(_, index_key)| BlockDeletion {
            object_key: index_key.clone(),
            sidecars: Vec::new(),
        })
        .collect();
    let manifests_retired = delete_blocks(store, &index_deletions).await;
    // A manifest that would not delete keeps its block, exactly as the
    // retention pass does: the index entry is still live, and the block it
    // names must still be there.
    let unretired: BTreeSet<&str> = manifests_retired
        .failures
        .iter()
        .map(|failure| failure.failed_key.as_str())
        .collect();

    // Step 4, for the passes before this one.
    let due: Vec<BlockDeletion> = deferred
        .take()
        .into_iter()
        .map(|object_key| BlockDeletion {
            object_key,
            sidecars: Vec::new(),
        })
        .collect();
    pass.blocks_deleted = delete_blocks(store, &due).await.into();
    deferred.extend(
        retired
            .into_iter()
            .filter(|(_, index_key)| !unretired.contains(index_key.as_str()))
            .map(|(block_key, _)| block_key),
    );
    pass.manifests_retired = manifests_retired.into();
    Ok(pass)
}

use super::{
    Arc, BlockStoreError, ByteSize, Future, IndexSnapshotRetain, ObjectStore, Path, PutMode,
    PutOptions, PutPayload, Result, SHARD_PAYLOAD_SWEEP_GRACE, SNAPSHOT_SWEEP_INTERVAL,
    SnapshotManifest, instrument, prune_old_index_snapshots, read_manifest_snapshot_base,
    snapshot_key_for_generation, sweep_orphan_shard_payloads,
};

/// Bound on optimistic retries within one snapshot write.
///
/// A losing attempt always means another writer's manifest landed, so the loop
/// only spins while the system as a whole makes progress. The bound turns a
/// store that never reports a conflict into a loud error instead of a hang.
const MAX_SNAPSHOT_WRITE_ATTEMPTS: usize = 64;

/// Folds a writer's index into the newest published manifest and swaps the
/// result in as the next generation.
///
/// The generation swaps the *manifest*, not the index. `merge` is handed the
/// manifest the attempt observed, writes whatever shard payloads its own
/// contribution changed, and returns the manifest that names them beside the
/// shards it did not touch. Payloads are content-addressed and immutable, so a
/// losing attempt leaves nothing to undo: its payloads are either named by the
/// manifest it retries with, or swept later as orphans.
///
/// The write is a conditional create on a key derived from the generation it
/// merged, so two writers that read the same base contend for the same key and
/// exactly one wins. The loser re-reads the winner's manifest and merges again,
/// which is what keeps a concurrent writer's blocks in the index instead of
/// silently overwriting them.
#[instrument(
    skip_all,
    fields(key = %key, retain = %retain, attempts = tracing::field::Empty, len = tracing::field::Empty),
    err
)]
pub(crate) async fn put_manifest_snapshot<Merge, Fut>(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    retain: IndexSnapshotRetain,
    max_bytes: ByteSize,
    label: &str,
    merge: Merge,
) -> Result<String>
where
    Merge: Fn(Option<SnapshotManifest>) -> Fut,
    Fut: Future<Output = Result<SnapshotManifest>>,
{
    for attempt in 1..=MAX_SNAPSHOT_WRITE_ATTEMPTS {
        let Some(base) = read_manifest_snapshot_base(store, key, max_bytes, label).await? else {
            tokio::task::yield_now().await;
            continue;
        };
        let generation = base.next_generation;
        let snapshot_key = snapshot_key_for_generation(key, generation);
        let bytes = merge(base.manifest).await?.to_bytes()?;
        let len = bytes.len();
        match store
            .put_opts(
                &Path::from(snapshot_key.clone()),
                PutPayload::from(bytes),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => {
                tracing::Span::current().record("attempts", attempt);
                tracing::Span::current().record("len", len);
                prune_old_index_snapshots(store, key, retain.into_value()).await?;
                if generation.is_multiple_of(SNAPSHOT_SWEEP_INTERVAL) {
                    sweep_orphan_shard_payloads(
                        store,
                        key,
                        max_bytes,
                        label,
                        SHARD_PAYLOAD_SWEEP_GRACE,
                    )
                    .await?;
                }
                return Ok(snapshot_key);
            }
            // Another writer claimed this generation. Its manifest is now the
            // merge base, so re-read and fold into that instead.
            Err(object_store::Error::AlreadyExists { .. }) => tokio::task::yield_now().await,
            Err(error) => return Err(error.into()),
        }
    }

    Err(BlockStoreError::ObjectStore(format!(
        "index snapshot `{key}` lost {MAX_SNAPSHOT_WRITE_ATTEMPTS} consecutive writes to concurrent writers"
    )))
}

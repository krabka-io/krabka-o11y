use super::{
    Arc, BlockStoreError, ByteSize, IndexSnapshotRetain, ObjectStore, Path, PutMode, PutOptions,
    PutPayload, Result, instrument, prune_old_index_snapshots, read_index_snapshot_base,
    snapshot_key_for_generation,
};

/// Bound on optimistic retries within one snapshot write.
///
/// A losing attempt always means another writer's snapshot landed, so the loop
/// only spins while the system as a whole makes progress. The bound turns a
/// store that never reports a conflict into a loud error instead of a hang.
const MAX_SNAPSHOT_WRITE_ATTEMPTS: usize = 64;

/// Folds a writer's index into the newest stored snapshot and publishes the
/// result as the next generation.
///
/// The write is a conditional create on a key derived from the generation it
/// merged, so two writers that read the same base contend for the same key and
/// exactly one wins. The loser re-reads the winner's snapshot and merges again,
/// which is what keeps a concurrent writer's blocks in the index instead of
/// silently overwriting them.
///
/// `merge` receives the serialised merge base, or `None` when the key has no
/// snapshot and no single-object index yet, and returns the bytes to store. It
/// runs once per attempt, always against the base that attempt observed.
#[instrument(
    skip_all,
    fields(key = %key, retain = %retain, attempts = tracing::field::Empty),
    err
)]
pub(crate) async fn put_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    retain: IndexSnapshotRetain,
    max_bytes: ByteSize,
    label: &str,
    merge: impl Fn(Option<&[u8]>) -> Result<Vec<u8>>,
) -> Result<String> {
    for attempt in 1..=MAX_SNAPSHOT_WRITE_ATTEMPTS {
        let Some(base) = read_index_snapshot_base(store, key, max_bytes, label).await? else {
            tokio::task::yield_now().await;
            continue;
        };
        let snapshot_key = snapshot_key_for_generation(key, base.next_generation);
        let payload = PutPayload::from(merge(base.bytes.as_deref())?);
        match store
            .put_opts(
                &Path::from(snapshot_key.clone()),
                payload,
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => {
                tracing::Span::current().record("attempts", attempt);
                prune_old_index_snapshots(store, key, retain.into_value()).await?;
                return Ok(snapshot_key);
            }
            // Another writer claimed this generation. Its snapshot is now the
            // merge base, so re-read and fold into that instead.
            Err(object_store::Error::AlreadyExists { .. }) => tokio::task::yield_now().await,
            Err(error) => return Err(error.into()),
        }
    }

    Err(BlockStoreError::ObjectStore(format!(
        "index snapshot `{key}` lost {MAX_SNAPSHOT_WRITE_ATTEMPTS} consecutive writes to concurrent writers"
    )))
}

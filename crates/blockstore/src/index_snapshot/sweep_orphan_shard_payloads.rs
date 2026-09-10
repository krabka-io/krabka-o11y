use futures::StreamExt as _;

use super::{
    Arc, BTreeSet, ByteSize, Duration, IndexSnapshotBytes, ObjectStore, ObjectStoreExt as _, Path,
    Result, SnapshotManifest, SystemTime, UNIX_EPOCH, instrument, is_shard_payload_location,
    list_index_snapshot_objects, read_index_snapshot_bytes, shard_payload_prefix_for_key,
};

/// Deletes the shard payloads of `key` that no retained manifest names.
///
/// Payloads are immutable and content-addressed, so a shard that changes
/// leaves its previous payload behind, still named by the generations that
/// published it. Once those generations are pruned the payload is unreachable,
/// and this is what reclaims it.
///
/// `grace` is what keeps the sweep from racing a writer: a payload that has
/// been written more recently than this is left alone, because a writer that
/// has put its payloads and not yet swapped its manifest is indistinguishable
/// from one that never will. A payload the sweep cannot date is kept.
///
/// The sweep deletes only objects that spell a payload key of this index, and
/// leaves anything else under the prefix where it is: the prefix belongs to the
/// index, but a bucket is shared.
#[instrument(
    level = "debug",
    skip_all,
    fields(key = %key, listed = tracing::field::Empty, deleted = tracing::field::Empty),
    err
)]
pub(crate) async fn sweep_orphan_shard_payloads(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: ByteSize,
    label: &str,
    grace: Duration,
) -> Result<()> {
    let mut referenced = BTreeSet::new();
    for meta in list_index_snapshot_objects(store, key).await? {
        // A manifest pruned out from under the listing referenced only what an
        // older generation did, so skipping it cannot orphan a live payload.
        if let IndexSnapshotBytes::Present(bytes) =
            read_index_snapshot_bytes(store, &meta.location, max_bytes, label).await?
        {
            referenced
                .extend(SnapshotManifest::from_bytes(label, &bytes)?.payload_object_keys(key));
        }
    }

    let cutoff = SystemTime::now()
        .checked_sub(grace)
        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_secs()).ok());
    let mut listed = 0_usize;
    let mut stale = Vec::new();
    let mut listing = store.list(Some(&Path::from(shard_payload_prefix_for_key(key))));
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        if !is_shard_payload_location(key, meta.location.as_ref()) {
            continue;
        }
        listed += 1;
        if referenced.contains(meta.location.as_ref()) {
            continue;
        }
        // At or before the cutoff, not strictly before it: object timestamps
        // are whole seconds, and a zero grace has to mean "sweep everything"
        // rather than "sweep everything written in a previous second".
        if cutoff.is_some_and(|cutoff| meta.last_modified.timestamp() <= cutoff) {
            stale.push(meta.location);
        }
    }

    tracing::Span::current().record("listed", listed);
    tracing::Span::current().record("deleted", stale.len());
    for location in stale {
        match store.delete(&location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

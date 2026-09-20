use futures::StreamExt as _;

use super::{
    Arc, BTreeSet, ByteSize, Duration, IndexSnapshotBytes, ObjectMeta, ObjectStore, Path, PutMode,
    PutOptions, PutPayload, Result, SnapshotManifest, SystemTime, UNIX_EPOCH, UpdateVersion,
    instrument, is_shard_payload_location, list_index_snapshot_objects, read_index_snapshot_bytes,
    shard_payload_prefix_for_key,
};

async fn reclaim_if_unchanged(store: &Arc<dyn ObjectStore>, meta: ObjectMeta) -> Result<bool> {
    // ponytail: tombstones retain listing entries; use unique manifest payload
    // IDs before deleting keys if empty-object accumulation becomes material.
    let version = UpdateVersion {
        e_tag: meta.e_tag,
        version: meta.version,
    };
    match store
        .put_opts(
            &meta.location,
            PutPayload::from_static(b""),
            PutOptions::from(PutMode::Update(version)),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. }) => {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

/// Reclaims the shard payloads of `key` that no retained manifest names.
///
/// Payloads are immutable and content-addressed, so a shard that changes
/// leaves its previous payload behind, still named by the generations that
/// published it. Once those generations are pruned the payload is unreachable,
/// and this is what reclaims its bytes.
///
/// Reclamation conditionally replaces the listed object version with an empty
/// tombstone. If a writer has restored that key since the listing, the version
/// precondition fails and the writer's bytes survive.
///
/// `grace` is what keeps the sweep from racing a writer: a payload that has
/// been written more recently than this is left alone, because a writer that
/// has put its payloads and not yet swapped its manifest is indistinguishable
/// from one that never will. A payload the sweep cannot date is kept.
///
/// The sweep reclaims only objects that spell a payload key of this index, and
/// leaves anything else under the prefix where it is: the prefix belongs to the
/// index, but a bucket is shared.
#[instrument(
    level = "debug",
    skip_all,
    fields(key = %key, listed = tracing::field::Empty, reclaimed = tracing::field::Empty),
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
        if meta.size > 0 && cutoff.is_some_and(|cutoff| meta.last_modified.timestamp() <= cutoff) {
            stale.push(meta);
        }
    }

    tracing::Span::current().record("listed", listed);
    let mut reclaimed = 0_usize;
    for meta in stale {
        // A writer may have replaced this content-addressed key after the
        // listing. The stale version, not merely its path, is the candidate.
        if reclaim_if_unchanged(store, meta).await? {
            reclaimed += 1;
        }
    }
    tracing::Span::current().record("reclaimed", reclaimed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt as _, memory::InMemory};

    use super::*;

    #[tokio::test]
    async fn a_rewrite_invalidates_a_stale_sweep_selection() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = Path::from("payload.kbs");
        store
            .put(&location, PutPayload::from_static(b"old"))
            .await
            .unwrap();
        let selected = store.head(&location).await.unwrap();
        store
            .put(&location, PutPayload::from_static(b"new"))
            .await
            .unwrap();

        assert2::check!(!reclaim_if_unchanged(&store, selected).await.unwrap());
        assert2::check!(store.get(&location).await.unwrap().bytes().await.unwrap() == b"new"[..]);
    }
}

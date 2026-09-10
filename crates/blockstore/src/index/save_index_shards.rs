use super::{
    Arc, BTreeMap, BTreeSet, Index, IndexShardObject, IndexShardPayload, ObjectStore,
    ObjectStoreExt as _, Path, PutPayload, Result, StreamExt as _, encode_index_shard,
    index_shard_object_key, index_shards_prefix_for_key, index_unbound_series_object_key,
    instrument, parse_index_shard_location, plan_tenant_index_shards,
};

/// Writes every shard of `index` under the shard prefix of `key`, then deletes
/// the shards the new layout does not name.
///
/// The grid coarsens as a tenant's span grows, so a re-save can leave the
/// narrower shards behind. The sweep is what removes them, and it is also what
/// clears a tenant that no longer has blocks. The sweep deletes only the
/// objects this module recognises, and it leaves anything else under the
/// prefix where it is.
///
/// Two writers saving the same key concurrently still clobber each other, as
/// they did when the index was one object. The compare-and-swap publication in
/// [`crate::index_snapshot`] swaps a manifest that names content-addressed
/// shard payloads instead, which is what the traces and profiles indexes use;
/// the metrics path has not been moved onto it. See the module documentation
/// on [`Index`].
#[instrument(
    skip_all,
    fields(key = %key, width = width, shards = tracing::field::Empty, len = tracing::field::Empty),
    err
)]
pub(crate) async fn save_index_shards(
    index: &Index,
    store: &Arc<dyn ObjectStore>,
    key: &str,
    width: i64,
) -> Result<()> {
    let mut written = BTreeSet::new();
    let mut bytes_written = 0_usize;

    for (tenant, tenant_index) in &index.tenants {
        for (range, ordinals) in plan_tenant_index_shards(tenant_index, width) {
            // A shard is a document of its own, so its blocks are renumbered
            // from zero and its postings point at those numbers.
            let local = ordinals
                .iter()
                .enumerate()
                .map(|(local, ordinal)| {
                    (
                        *ordinal,
                        u32::try_from(local).expect("a shard holds fewer blocks than the tenant"),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let postings = tenant_index
                .blocks
                .postings_for(&ordinals)
                .into_iter()
                .map(|(fingerprint, ordinals)| {
                    let ordinals = ordinals
                        .iter()
                        .filter_map(|ordinal| local.get(ordinal).copied())
                        .collect::<Vec<_>>();
                    (fingerprint, ordinals)
                })
                .collect::<BTreeMap<_, _>>();
            let selected = postings
                .keys()
                .copied()
                .filter(|fingerprint| tenant_index.series.contains_key(fingerprint))
                .collect();
            let blocks = ordinals
                .iter()
                .map(|ordinal| tenant_index.blocks.entry(*ordinal))
                .collect();

            let payload = IndexShardPayload {
                tenant,
                series: &tenant_index.series,
                selected,
                blocks,
                postings,
            };
            let object_key = index_shard_object_key(key, tenant, range);
            bytes_written += put(store, &object_key, encode_index_shard(&payload)).await?;
            written.insert(object_key);
        }

        let bound = tenant_index.blocks.live_fingerprints();
        let unbound = tenant_index
            .series
            .keys()
            .copied()
            .filter(|fingerprint| !bound.contains(fingerprint))
            .collect::<BTreeSet<_>>();
        if !unbound.is_empty() {
            let payload = IndexShardPayload {
                tenant,
                series: &tenant_index.series,
                selected: unbound,
                blocks: Vec::new(),
                postings: BTreeMap::new(),
            };
            let object_key = index_unbound_series_object_key(key, tenant);
            bytes_written += put(store, &object_key, encode_index_shard(&payload)).await?;
            written.insert(object_key);
        }
    }

    tracing::Span::current().record("shards", written.len());
    tracing::Span::current().record("len", bytes_written);
    prune_unnamed_shards(store, key, &written).await
}

async fn put(store: &Arc<dyn ObjectStore>, object_key: &str, bytes: Vec<u8>) -> Result<usize> {
    let len = bytes.len();
    store
        .put(&Path::from(object_key), PutPayload::from(bytes))
        .await?;
    Ok(len)
}

async fn prune_unnamed_shards(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    written: &BTreeSet<String>,
) -> Result<()> {
    let shards_prefix = index_shards_prefix_for_key(key);
    let mut stale = Vec::new();
    let mut listing = store.list(Some(&Path::from(shards_prefix.clone())));
    while let Some(meta) = listing.next().await {
        let location = meta?.location;
        let Some((_, object)) = parse_index_shard_location(&shards_prefix, location.as_ref())
        else {
            continue;
        };
        if matches!(object, IndexShardObject::Foreign) {
            continue;
        }
        if !written.contains(location.as_ref()) {
            stale.push(location);
        }
    }
    for location in stale {
        store.delete(&location).await?;
    }
    Ok(())
}

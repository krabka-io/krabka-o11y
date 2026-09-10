use super::{
    Arc, ByteSize, Index, IndexShardObject, ObjectStore, Path, Result, StreamExt as _,
    decode_index_shard, index_shard_tenant_prefix, index_shards_prefix_for_key, instrument,
    parse_index_shard_location, read_index_shard,
};

/// Reads the shards of `key` that a query needs and folds them into one index.
///
/// `tenant` narrows the listing to one tenant's prefix, and `window` drops the
/// shards whose span cannot meet the query's. What is left is what gets read,
/// which is the whole point of the layout: the shards outside the window are
/// never fetched and never held.
///
/// [`super::MAX_INDEX_SHARDS_PER_TENANT`] bounds a tenant's shards, so the
/// listing is one page and an offset would save nothing. An offset would also
/// have to guess how wide the first shard is. That guess is what makes such a
/// skip unsound.
#[instrument(
    level = "debug",
    skip_all,
    fields(
        key = %key,
        tenant = tracing::field::Empty,
        listed = tracing::field::Empty,
        read = tracing::field::Empty,
    ),
    err
)]
pub(crate) async fn load_index_shards(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    tenant: Option<&str>,
    window: Option<(i64, i64)>,
    max_bytes: ByteSize,
) -> Result<Index> {
    if let Some(tenant) = tenant {
        tracing::Span::current().record("tenant", tenant);
    }
    let shards_prefix = index_shards_prefix_for_key(key);
    let listing_prefix = match tenant {
        Some(tenant) => index_shard_tenant_prefix(key, tenant),
        None => shards_prefix.clone(),
    };

    let mut listed = 0_usize;
    let mut wanted = Vec::new();
    let mut listing = store.list(Some(&Path::from(listing_prefix)));
    while let Some(meta) = listing.next().await {
        let location = meta?.location;
        let Some((_, object)) = parse_index_shard_location(&shards_prefix, location.as_ref())
        else {
            continue;
        };
        match object {
            IndexShardObject::Shard(range) => {
                listed += 1;
                if window.is_none_or(|(min_ts, max_ts)| range.overlaps(min_ts, max_ts)) {
                    wanted.push(location);
                }
            }
            // A series no block carries yet has no span to compare against, so
            // it is read whatever the window is.
            IndexShardObject::UnboundSeries => {
                listed += 1;
                wanted.push(location);
            }
            IndexShardObject::Foreign => {}
        }
    }
    tracing::Span::current().record("listed", listed);
    tracing::Span::current().record("read", wanted.len());

    let mut index = Index::new();
    for location in wanted {
        let bytes = read_index_shard(store, &location, max_bytes).await?;
        let shard = decode_index_shard(location.as_ref(), &bytes)?;
        index.merge_from(&shard);
    }
    Ok(index)
}

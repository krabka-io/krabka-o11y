use super::*;

/// Reads every `.index` manifest and publication marker under the metrics
/// prefix, in key order.
///
/// Metrics holds no index in memory. The manifests beside the blocks are the
/// index, so a caller that wants to know which blocks are live reads them all.
/// The whole prefix is listed rather than one prefix per tenant: a tenant with
/// no entry in the overrides file still has blocks, and a per-tenant listing
/// would never reach it.
///
/// Each manifest must name the key it was read from. A manifest found under
/// another key describes another block, and a caller that deleted or merged
/// what it names could lose a live block, so the listing fails instead.
///
/// # Errors
/// Returns [`CompactionManifestError::ObjectStore`] when the prefix cannot be
/// listed or a manifest cannot be fetched, [`CompactionManifestError::Index`]
/// when a manifest cannot be decoded, and
/// [`CompactionManifestError::KeyMismatch`] when a manifest does not name the
/// key it was read from.
pub async fn list_compaction_index(
    store: &Arc<dyn ObjectStore>,
) -> Result<CompactionIndex, CompactionManifestError> {
    let listing = list_compaction_index_objects(store).await?;
    Ok(CompactionIndex {
        live: read_compaction_manifests(store, listing.live).await?,
        pending: read_compaction_manifests(store, listing.pending).await?,
        markers: listing
            .markers
            .into_iter()
            .map(|object| object.location.to_string())
            .collect(),
    })
}

/// Lists the metrics prefix in key order, and splits the manifests and
/// publication markers by [`CompactionIndexListing`].
pub(super) async fn list_compaction_index_objects(
    store: &Arc<dyn ObjectStore>,
) -> Result<CompactionIndexListing<ObjectMeta>, CompactionManifestError> {
    let mut objects = store
        .list(Some(&Path::from(COMPACTION_OBJECT_PREFIX)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?;
    objects.sort_by(|left, right| left.location.cmp(&right.location));
    Ok(CompactionIndexListing::new(objects, |object| {
        object.location.as_ref()
    }))
}

/// Reads every live `.index` manifest under the metrics prefix, in key order.
///
/// A live manifest is one that queries read. The manifests of a TSDB import
/// that is not published yet are not live, and this function does not fetch
/// them. See [`list_compaction_index`](super::list_compaction_index) for the
/// listing and the checks.
///
/// # Errors
/// Returns the errors of [`list_compaction_index`](super::list_compaction_index).
pub async fn list_compaction_manifests(
    store: &Arc<dyn ObjectStore>,
) -> Result<Vec<CompactionIndexManifest>, CompactionManifestError> {
    let listing = list_compaction_index_objects(store).await?;
    read_compaction_manifests(store, listing.live).await
}

/// Reads the `.index` manifests at `objects`, in their order.
///
/// Each manifest must name the key it was read from. A manifest found under
/// another key describes another block, and a caller that deleted or merged
/// what it names could lose a live block, so the read fails instead.
pub(super) async fn read_compaction_manifests(
    store: &Arc<dyn ObjectStore>,
    objects: Vec<ObjectMeta>,
) -> Result<Vec<CompactionIndexManifest>, CompactionManifestError> {
    let mut manifests = Vec::with_capacity(objects.len());
    for object in objects {
        let key = object.location.as_ref();
        let bytes = store
            .get(&object.location)
            .await
            .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| CompactionManifestError::ObjectStore(error.to_string()))?;
        let manifest = CompactionIndexManifest::decode(&bytes)?;
        if manifest.index_key != key {
            return Err(CompactionManifestError::KeyMismatch {
                listed: key.to_string(),
                manifest: manifest.index_key,
            });
        }
        manifests.push(manifest);
    }
    Ok(manifests)
}

use krabka_metrics::CompactionIndexListing;

use super::{
    Arc, BTreeMap, BTreeSet, CompactionIndexManifest, MetricsServiceError, ObjectStore,
    ObjectStoreExt, Path, TryStreamExt,
};

#[tracing::instrument(
    level = "debug",
    name = "metrics.manifests.load",
    skip_all,
    fields(prefix = %manifest_prefix, manifests = tracing::field::Empty),
    err
)]
pub(crate) async fn load_compaction_manifests_filtered_with_cache(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    time_range: Option<(i64, i64)>,
    cache: Option<&tokio::sync::RwLock<BTreeMap<String, CompactionIndexManifest>>>,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    let prefix = (!manifest_prefix.is_empty()).then(|| Path::from(manifest_prefix));
    let mut retired_missing: Option<(String, object_store::Error)> = None;
    let mut retries = 2;
    loop {
        let mut objects = store.list(prefix.as_ref()).try_collect::<Vec<_>>().await?;
        objects.sort_by(|left, right| left.location.cmp(&right.location));
        // An import is visible only with its publication marker in this listing.
        let objects = CompactionIndexListing::new(objects, |object| object.location.as_ref()).live;
        let live_keys = objects
            .iter()
            .map(|object| object.location.as_ref().to_string())
            .collect::<BTreeSet<_>>();
        if let Some((key, error)) = retired_missing.take()
            && live_keys.contains(&key)
        {
            // Still listed: this is missing current data, not retirement.
            return Err(error.into());
        }
        let mut manifests = Vec::new();
        let mut fetched = Vec::<(String, CompactionIndexManifest)>::new();
        for object in objects {
            let key = object.location.as_ref();
            let manifest = if let Some(cache) = cache
                && let Some(manifest) = cache.read().await.get(key).cloned()
            {
                manifest
            } else {
                let bytes = match async { store.get(&object.location).await?.bytes().await }.await {
                    Ok(bytes) => bytes,
                    Err(error @ object_store::Error::NotFound { .. }) if retries > 0 => {
                        retired_missing = Some((key.to_string(), error));
                        break;
                    }
                    Err(error) => return Err(error.into()),
                };
                let manifest = CompactionIndexManifest::decode(&bytes)?;
                fetched.push((key.to_string(), manifest.clone()));
                manifest
            };
            // Metadata has no event timestamp, so its zero bounds are timeless.
            if manifest.kind == krabka_metrics::MetricBlockKind::Metadata
                || time_range.is_none_or(|(start_ms, end_ms)| {
                    manifest.max_ts >= start_ms && manifest.min_ts <= end_ms
                })
            {
                manifests.push(manifest);
            }
        }
        if retired_missing.is_some() {
            // Rebuild the whole listing so replacement blocks are included.
            retries -= 1;
            continue;
        }
        if let Some(cache) = cache {
            let mut guard = cache.write().await;
            guard.retain(|key, _| live_keys.contains(key));
            guard.extend(fetched);
        }
        tracing::Span::current().record("manifests", manifests.len());
        return Ok(manifests);
    }
}

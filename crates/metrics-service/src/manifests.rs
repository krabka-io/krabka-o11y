//! Compaction manifest listing, filtering, caching, and refresh ranges.

use futures::StreamExt;
use krabka_metrics::{CompactionIndexListing, CompactionIndexManifest};

use super::{
    Arc, BTreeMap, BTreeSet, MetricsServiceError, ObjectStore, ObjectStoreExt, Path, Time, TimeExt,
    TryStreamExt,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn load_compaction_manifests(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered(store, manifest_prefix, None).await
}

pub(crate) async fn load_compaction_manifests_filtered(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    time_range: Option<ManifestTimeRange>,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    Ok(
        load_compaction_manifests_filtered_with_cache(store, manifest_prefix, time_range, None)
            .await?
            .into_iter()
            .map(Arc::unwrap_or_clone)
            .collect(),
    )
}

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
    time_range: Option<ManifestTimeRange>,
    cache: Option<&tokio::sync::RwLock<BTreeMap<String, Arc<CompactionIndexManifest>>>>,
) -> Result<Vec<Arc<CompactionIndexManifest>>, MetricsServiceError> {
    let prefix = (!manifest_prefix.is_empty()).then(|| Path::from(manifest_prefix));
    let mut retired_missing: Option<RetiredManifest> = None;
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
        if let Some(RetiredManifest { key, error }) = retired_missing.take()
            && live_keys.contains(&key)
        {
            // Still listed: this is missing current data, not retirement.
            return Err(error.into());
        }
        let mut manifests = Vec::new();
        let mut fetched = BTreeMap::new();
        // Keep listing order and retirement retries while overlapping bounded I/O.
        let mut loads = futures::stream::iter(objects)
            .map(|object| {
                let store = &store;
                async move {
                    let cached = if let Some(cache) = cache {
                        cache.read().await.get(object.location.as_ref()).cloned()
                    } else {
                        None
                    };
                    let bytes = if cached.is_none() {
                        Some(async { store.get(&object.location).await?.bytes().await }.await)
                    } else {
                        None
                    };
                    ManifestLoad {
                        object,
                        cached,
                        bytes,
                    }
                }
            })
            .buffered(4);
        while let Some(ManifestLoad {
            object,
            cached,
            bytes,
        }) = loads.next().await
        {
            let key = object.location.as_ref();
            let manifest = if let Some(manifest) = cached {
                manifest
            } else {
                let bytes = match bytes.expect("uncached manifest was read") {
                    Ok(bytes) => bytes,
                    Err(error @ object_store::Error::NotFound { .. }) if retries > 0 => {
                        retired_missing = Some(RetiredManifest {
                            key: key.to_string(),
                            error,
                        });
                        break;
                    }
                    Err(error) => return Err(error.into()),
                };
                let manifest = Arc::new(CompactionIndexManifest::decode(&bytes)?);
                fetched.insert(key.to_string(), Arc::clone(&manifest));
                manifest
            };
            // Metadata has no event timestamp, so its zero bounds are timeless.
            if manifest.kind == krabka_metrics::MetricBlockKind::Metadata
                || time_range.is_none_or(|range| {
                    manifest.max_ts >= range.start_ms && manifest.min_ts <= range.end_ms
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

struct RetiredManifest {
    key: String,
    error: object_store::Error,
}

struct ManifestLoad {
    object: object_store::ObjectMeta,
    cached: Option<Arc<CompactionIndexManifest>>,
    bytes: Option<Result<bytes::Bytes, object_store::Error>>,
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn load_compaction_manifests_for_range(
    store: Arc<dyn ObjectStore>,
    manifest_prefix: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<CompactionIndexManifest>, MetricsServiceError> {
    load_compaction_manifests_filtered(
        store,
        manifest_prefix,
        Some(ManifestTimeRange { start_ms, end_ms }),
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManifestTimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

pub(crate) fn normalize_refresh_range(
    start_ms: i64,
    end_ms: i64,
    lookback: Time,
    now_ms: i64,
) -> ManifestTimeRange {
    if start_ms == i64::MIN && end_ms == i64::MAX {
        return ManifestTimeRange {
            start_ms: now_ms.saturating_sub(lookback.millis_i64()),
            end_ms: i64::MAX,
        };
    }
    ManifestTimeRange { start_ms, end_ms }
}

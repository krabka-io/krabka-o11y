use super::*;

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
    let mut objects = store.list(prefix.as_ref()).try_collect::<Vec<_>>().await?;
    objects.sort_by(|left, right| left.location.cmp(&right.location));

    let objects = objects
        .into_iter()
        .filter(|object| {
            let key = object.location.as_ref();
            StdPath::new(key)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("index"))
        })
        .collect::<Vec<_>>();
    let live_keys = objects
        .iter()
        .map(|object| object.location.as_ref().to_string())
        .collect::<BTreeSet<_>>();
    let mut manifests = Vec::new();
    let mut fetched = Vec::<(String, CompactionIndexManifest)>::new();
    for object in objects {
        let key = object.location.as_ref();
        let manifest = if let Some(cache) = cache
            && let Some(manifest) = cache.read().await.get(key).cloned()
        {
            manifest
        } else {
            let bytes = store.get(&object.location).await?.bytes().await?;
            let manifest = CompactionIndexManifest::decode(&bytes)?;
            fetched.push((key.to_string(), manifest.clone()));
            manifest
        };
        if time_range.is_none_or(|(start_ms, end_ms)| {
            manifest.max_ts >= start_ms && manifest.min_ts <= end_ms
        }) {
            manifests.push(manifest);
        }
    }
    if let Some(cache) = cache {
        let mut guard = cache.write().await;
        guard.retain(|key, _| live_keys.contains(key));
        guard.extend(fetched);
    }
    tracing::Span::current().record("manifests", manifests.len());
    Ok(manifests)
}

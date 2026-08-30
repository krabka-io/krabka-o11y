use super::{Arc, ByteRateExt, ByteSizeExt, COMPACTION_OBJECT_PREFIX, CompactionIndexManifest, CompactionRetentionError, CompactionRetentionStats, FrequencyExt, ObjectStore, ObjectStoreExt, Path, RatioExt, Time, TimeExt, TryStreamExt, delete_if_exists};

/// Deletes compacted metric blocks whose index manifest ends before the
/// retention cutoff.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn enforce_compaction_retention(
    store: Arc<dyn ObjectStore>,
    now_ms: i64,
    retention: Time,
) -> Result<CompactionRetentionStats, CompactionRetentionError> {
    if retention <= Time::ZERO {
        return Ok(CompactionRetentionStats::default());
    }

    let cutoff_ms = now_ms.saturating_sub(retention.millis_i64());
    let mut objects = store
        .list(Some(&Path::from(COMPACTION_OBJECT_PREFIX)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| CompactionRetentionError::ObjectStore(error.to_string()))?;
    objects.sort_by(|left, right| left.location.cmp(&right.location));

    let mut stats = CompactionRetentionStats::default();
    for object in objects {
        let key = object.location.as_ref();
        if !std::path::Path::new(key)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("index"))
        {
            continue;
        }
        stats.manifests_scanned += 1;
        let bytes = store
            .get(&object.location)
            .await
            .map_err(|error| CompactionRetentionError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| CompactionRetentionError::ObjectStore(error.to_string()))?;
        let manifest = CompactionIndexManifest::decode(&bytes)?;
        if manifest.index_key != key {
            return Err(CompactionRetentionError::ManifestKeyMismatch {
                listed: key.to_string(),
                manifest: manifest.index_key,
            });
        }
        if manifest.max_ts >= cutoff_ms {
            continue;
        }

        if delete_if_exists(&store, &Path::from(manifest.index_key.clone())).await? {
            stats.manifests_deleted += 1;
        }
        if delete_if_exists(&store, &Path::from(manifest.block_key.clone())).await? {
            stats.blocks_deleted += 1;
        }
    }

    Ok(stats)
}

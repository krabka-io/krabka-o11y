use futures::StreamExt as _;

use super::{
    OBJECT_STORE_PROBE_PREFIX, ObjectMeta, ObjectStore, ObjectStoreError, ObjectStoreExt,
    ObjectStoreSemanticsError, Path, instrument,
};

/// The most bytes the read probe reads from an object.
const READ_PROBE_BYTES: u64 = 4;

/// Checks that a read-only role can list and read `store` below `prefix`,
/// and writes nothing.
///
/// The probe lists `prefix` and takes the first object that is not a probe
/// object. When there is one, it reads the first bytes of that object, up to
/// four, as a bounded range and checks that it gets that many bytes. An empty
/// prefix passes on the listing alone, because a new deployment has no
/// objects yet. An object that is deleted between the listing and the read
/// also passes, because a compactor can remove a block at any time.
///
/// The probe needs only list and get, so it passes on a read-only credential.
/// It does not check create-if-absent, list-after-write, conditional update or
/// delete, because they need a write. The roles that write run
/// [`verify_object_store_semantics`](super::verify_object_store_semantics).
///
/// # Errors
/// Returns [`ObjectStoreSemanticsError::RangedReadMismatch`] when the range
/// read returns the wrong number of bytes, or
/// [`ObjectStoreSemanticsError::ObjectStore`] when the listing or the read
/// fails, such as for a bad credential or a missing bucket.
#[instrument(level = "info", skip_all, fields(store = %store, prefix = %prefix), err)]
pub async fn verify_object_store_read_access(
    store: &dyn ObjectStore,
    prefix: &Path,
) -> Result<(), ObjectStoreSemanticsError> {
    let Some(sample) = first_object(store, prefix).await? else {
        tracing::info!("object store read access verified on an empty prefix");
        return Ok(());
    };
    let end = sample.size.min(READ_PROBE_BYTES);
    match store.get_range(&sample.location, 0..end).await {
        Ok(bytes) if bytes.len() as u64 == end => {}
        Ok(_) => {
            return Err(ObjectStoreSemanticsError::RangedReadMismatch {
                store: store.to_string(),
                range: "bounded",
            });
        }
        Err(ObjectStoreError::NotFound { .. }) => {}
        Err(source) => {
            return Err(ObjectStoreSemanticsError::ObjectStore {
                step: "read a byte range of a listed object",
                source,
            });
        }
    }
    tracing::info!(sample = %sample.location, "object store read access verified");
    Ok(())
}

/// The first listed object below `prefix` that is not a probe object.
async fn first_object(
    store: &dyn ObjectStore,
    prefix: &Path,
) -> Result<Option<ObjectMeta>, ObjectStoreSemanticsError> {
    let probes = prefix.clone().join(OBJECT_STORE_PROBE_PREFIX);
    let mut listing = store.list(Some(prefix));
    while let Some(entry) = listing.next().await {
        let meta = entry.map_err(|source| ObjectStoreSemanticsError::ObjectStore {
            step: "list the configured prefix",
            source,
        })?;
        if !meta.location.prefix_matches(&probes) && meta.size > 0 {
            return Ok(Some(meta));
        }
    }
    Ok(None)
}

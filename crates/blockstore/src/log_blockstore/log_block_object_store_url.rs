use super::{AtomicU64, BlockStoreError, ObjectStoreUrl, Ordering};

/// Hands out the `DataFusion` object-store URL for one planned scan.
///
/// A [`LogBlockTableProvider`] built over an object store holds an
/// `Arc<dyn ObjectStore>`, not a location: nothing about the store says what
/// URL it should answer to, and two providers in one process may hold two
/// different stores. So each provider mints its own authority and registers
/// its store under it when it scans. The counter only has to keep two
/// providers in the same process apart, which `Relaxed` ordering is enough
/// for -- the value is never compared against anything but itself.
///
/// [`LogBlockTableProvider`]: super::LogBlockTableProvider
pub(crate) fn next_log_block_object_store_url() -> Result<ObjectStoreUrl, BlockStoreError> {
    static NEXT_AUTHORITY: AtomicU64 = AtomicU64::new(0);

    let authority = NEXT_AUTHORITY.fetch_add(1, Ordering::Relaxed);
    Ok(ObjectStoreUrl::parse(format!(
        "krabka-log-block://b{authority}"
    ))?)
}

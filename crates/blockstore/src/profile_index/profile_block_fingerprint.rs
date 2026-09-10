use super::{BlockMeta, DefaultHasher, Hash as _, Hasher as _};

/// Fingerprint of the profile-block record a writer holds.
///
/// A pending removal pins itself to this, so replaying the removal against a
/// merge base cannot drop a *different* block that has since been written under
/// the same object key. Records are only ever compared inside one process, so
/// the hasher does not have to be stable across builds or hosts; it only has to
/// be a function of everything the record says. The fields are named one by one
/// rather than hashing [`BlockMeta`] wholesale, so a field added there is a
/// deliberate choice here rather than a silent change of identity.
pub(crate) fn profile_block_fingerprint(meta: &BlockMeta, partitions: &[u64]) -> u64 {
    let mut hasher = DefaultHasher::new();
    meta.tenant.hash(&mut hasher);
    meta.object_key.hash(&mut hasher);
    meta.min_ts.hash(&mut hasher);
    meta.max_ts.hash(&mut hasher);
    meta.row_count.hash(&mut hasher);
    meta.fingerprints.hash(&mut hasher);
    meta.level.hash(&mut hasher);
    partitions.hash(&mut hasher);
    hasher.finish()
}

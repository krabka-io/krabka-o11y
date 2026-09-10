use super::ObjectMeta;

/// Whether two [`ObjectMeta`]s describe the same bytes at the same key.
///
/// Every field the backend can offer as a validator has to agree. Where the
/// store supplies an `ETag` or a version, that settles it; where it does not,
/// both sides are `None` and the size and modification time carry the check.
/// The comparison is deliberately conservative: a false negative costs one
/// footer read, and a false positive answers a query from the wrong bytes.
pub(crate) fn same_object(cached: &ObjectMeta, current: &ObjectMeta) -> bool {
    cached.location == current.location
        && cached.size == current.size
        && cached.last_modified == current.last_modified
        && cached.e_tag == current.e_tag
        && cached.version == current.version
}

use super::{escape_object_path_segment, index_shards_prefix_for_key};

/// Object prefix holding one tenant's index shards.
///
/// The tenant is one path segment, as it is on the logs path, so a
/// tenant-scoped load lists its own prefix and never sees another tenant's
/// objects. [`escape_object_path_segment`] is what holds the tenant inside
/// that one segment: a name carrying `/`, `.` or `..` would otherwise reach
/// another tenant's prefix.
#[must_use]
pub fn index_shard_tenant_prefix(key: &str, tenant: &str) -> String {
    format!(
        "{}/tenant={}",
        index_shards_prefix_for_key(key),
        escape_object_path_segment(tenant)
    )
}

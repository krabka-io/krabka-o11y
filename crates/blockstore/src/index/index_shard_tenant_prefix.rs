use super::index_shards_prefix_for_key;

/// Object prefix holding one tenant's index shards.
///
/// The tenant is a path segment, as it is on the logs path, so a tenant-scoped
/// load lists its own prefix and never sees another tenant's objects. A tenant
/// name carrying `/` would split into two segments here; the logs path has the
/// same exposure and the same expectation that tenant names are plain.
#[must_use]
pub fn index_shard_tenant_prefix(key: &str, tenant: &str) -> String {
    format!("{}/tenant={tenant}", index_shards_prefix_for_key(key))
}

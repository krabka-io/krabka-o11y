use super::{IndexShardRange, index_shard_tenant_prefix, shard_bound_key};

/// Object key of one tenant's shard for `range`.
#[must_use]
pub fn index_shard_object_key(key: &str, tenant: &str, range: IndexShardRange) -> String {
    format!(
        "{}/time={}-{}/shard.kbi",
        index_shard_tenant_prefix(key, tenant),
        shard_bound_key(range.start),
        shard_bound_key(range.end),
    )
}

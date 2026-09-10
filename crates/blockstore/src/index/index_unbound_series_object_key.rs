use super::index_shard_tenant_prefix;

/// Object key of the tenant's unbound-series object.
///
/// A writer names its head series as it ingests them, so a series can exist
/// before any block carries it. Such a series belongs to no shard, because the
/// shards are cut on the time spans of blocks. To drop it would lose every
/// series a writer has not yet flushed. To copy it into every shard would
/// multiply it by the retention. It goes here instead, and every load reads
/// it.
///
/// The name sorts after `time=`, so a listing that starts at a `time=` offset
/// still reaches it.
#[must_use]
pub fn index_unbound_series_object_key(key: &str, tenant: &str) -> String {
    format!("{}/unbound.kbi", index_shard_tenant_prefix(key, tenant))
}

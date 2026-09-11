use super::{AclSet, BTreeMap, TenantId, async_trait};

/// The broker lookups that tenant authorization and ingest quotas read.
///
/// [`AdminBrokerAccess`](super::AdminBrokerAccess) asks a broker.
/// [`BrokerAccessCache`](super::BrokerAccessCache) holds the answers, so a
/// request reads memory and not the broker. The seam also lets a test count
/// how often the cache asks.
#[async_trait]
pub(crate) trait BrokerAccessSource: Send + Sync + 'static {
    /// The ACLs that can apply to `wal_topic`, or why the broker did not
    /// answer.
    async fn wal_topic_acls(&self, wal_topic: &str) -> Result<AclSet, String>;

    /// The client quotas the broker holds for `tenant`, or why the broker did
    /// not answer. An empty map means that the tenant has no quota.
    async fn user_quotas(&self, tenant: &TenantId) -> Result<BTreeMap<String, f64>, String>;
}

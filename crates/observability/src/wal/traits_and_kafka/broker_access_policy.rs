use super::{NonZeroUsize, ServiceConfig, ServiceConfigError, Time};

/// How long [`BrokerAccessCache`](super::BrokerAccessCache) trusts a broker
/// answer, and how many tenants' quotas it holds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct BrokerAccessPolicy {
    /// The age at which a snapshot is refreshed.
    pub(crate) ttl: Time,
    /// The age above which a snapshot is not served. A check then fails
    /// closed until the broker answers again.
    pub(crate) max_staleness: Time,
    /// The most tenants whose quota snapshot and rate bucket are held.
    pub(crate) tenant_capacity: NonZeroUsize,
}

impl BrokerAccessPolicy {
    /// The policy that `config` sets.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceConfigError::BrokerAccessStalenessBelowTtl`] when the
    /// staleness bound is shorter than the TTL, because a snapshot would then
    /// stop being served before its first refresh is due.
    pub(crate) fn for_config(config: &ServiceConfig) -> Result<Self, ServiceConfigError> {
        if config.broker_access_max_staleness < config.broker_access_cache_ttl {
            return Err(ServiceConfigError::BrokerAccessStalenessBelowTtl);
        }
        Ok(Self {
            ttl: config.broker_access_cache_ttl,
            max_staleness: config.broker_access_max_staleness,
            tenant_capacity: config.broker_access_tenant_capacity,
        })
    }
}

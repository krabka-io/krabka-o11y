use super::*;

/// A snapshot that stopped being served before its first refresh was due
/// would fail every check closed between refreshes, so the configuration is
/// refused.
#[test]
pub(crate) fn the_staleness_bound_may_not_be_shorter_than_the_ttl() {
    let config = ServiceConfig {
        broker_access_cache_ttl: secs(30),
        broker_access_max_staleness: secs(29),
        ..ServiceConfig::default()
    };
    check!(matches!(
        BrokerAccessPolicy::for_config(&config),
        Err(ServiceConfigError::BrokerAccessStalenessBelowTtl)
    ));

    let config = ServiceConfig {
        broker_access_cache_ttl: secs(30),
        broker_access_max_staleness: secs(30),
        ..ServiceConfig::default()
    };
    check!(
        BrokerAccessPolicy::for_config(&config).ok()
            == Some(BrokerAccessPolicy {
                ttl: secs(30),
                max_staleness: secs(30),
                tenant_capacity: NonZeroUsize::new(10_000).expect("a nonzero capacity"),
            })
    );
}

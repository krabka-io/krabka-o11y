use std::{sync::atomic::AtomicUsize, time::Duration};

use crate::tests::prelude::{
    AclEntry, AclOperation, AclSet, AdminError, Arc, AtomicBool, AtomicOrdering, AuthMethod,
    BTreeMap, BrokerAccessCache, BrokerAccessPolicy, BrokerAccessSource, BrokerBackedIngestLimiter,
    BrokerBackedQueryAuthorizer, CachedFailure, CachedLookup, CachedLookupDecision, CachedSnapshot,
    CancellationToken, IngestLimitError, Instant, LogIngestLimiter, LogQueryAuthorizer, Mutex,
    NonZeroUsize, PatternType, PermissionType, Principal, QueryAuthorizationError, ResourceType,
    SecurityEventSink, ServiceConfig, ServiceConfigError, TenantGrant, TenantId, TenantLru, Time,
    TimeExt, acl_set_from_describe, async_trait, check, check_tenant_wal_read_acl,
    check_tenant_wal_write_acl, millis, secs, wal_topic_acl_filters,
};

mod broker_access_for_test;
mod counting_broker_access;
mod only_security_disabled_turns_a_describe_error_into_an_allow;
mod the_quota_cache_and_the_rate_buckets_stay_within_the_tenant_capacity {
    use super::*;
    use crate::tests::api_record_for_test;

    /// A client can name any number of tenants. The quota snapshots and the rate
    /// buckets hold at most the configured count, so a spray of tenant names
    /// cannot grow the distributor's memory.
    #[tokio::test]
    pub(crate) async fn the_quota_cache_and_the_rate_buckets_stay_within_the_tenant_capacity() {
        let source = Arc::new(CountingBrokerAccess::answering(Ok(
            AclSet::SecurityDisabled,
        )));
        let mut quotas = BTreeMap::new();
        quotas.insert("producer_byte_rate".to_string(), 1_000_000.0);
        *source.quotas.lock().expect("fake quota lock") = Ok(quotas);
        let access = broker_access_for_test(&source, 3).cache;
        let limiter = BrokerBackedIngestLimiter::with_access(access, secs(1));

        for index in 0..50 {
            let name = format!("tenant-{index}");
            let tenant = tenant_for_test(&name);
            check!(
                limiter
                    .check(
                        &Principal::Unauthenticated,
                        &tenant,
                        &[api_record_for_test(&name)]
                    )
                    .await
                    .is_ok()
            );
        }

        check!(
            limiter
                .access
                .quotas
                .lock()
                .expect("quota cache lock")
                .entries
                .len()
                == 3
        );
        let buckets = limiter.buckets.lock().expect("bucket lock");
        check!(
            buckets
                .entries
                .keys()
                .map(TenantId::as_str)
                .collect::<Vec<_>>()
                == ["tenant-47", "tenant-48", "tenant-49"]
        );
        check!(source.lookups() == BrokerLookups { acl: 1, quota: 50 });
    }
}
mod the_wal_topic_acls_decide_by_their_state_and_the_principal_kind;

pub(crate) use broker_access_for_test::{
    broker_access_for_test, policy_for_test, security_disabled_broker_access, tenant_for_test,
};
pub(crate) use counting_broker_access::{BrokerLookups, CountingBrokerAccess};

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

/// A read marks a tenant as used, so the tenant removed at the capacity is the
/// one no request touched for longest, not the one inserted first.
#[test]
pub(crate) fn the_tenant_lru_removes_the_least_recently_used_tenant() {
    let mut lru = TenantLru::new(NonZeroUsize::new(2).expect("a nonzero capacity"));
    let a = tenant_for_test("a");
    let b = tenant_for_test("b");
    let c = tenant_for_test("c");
    *lru.get_or_insert_with(&a, || 0) += 1;
    *lru.get_or_insert_with(&b, || 0) += 1;
    check!(lru.get_mut(&a).copied() == Some(1));
    *lru.get_or_insert_with(&c, || 0) += 1;
    *lru.get_or_insert_with(&a, || 0) += 1;

    check!(lru.get_mut(&b).is_none());
    check!(lru.get_mut(&a).copied() == Some(2));
    check!(lru.get_mut(&c).copied() == Some(1));
    check!(lru.entries.len() == 2);
    check!(lru.recency.len() == 2);
}

/// The admin client has no `MATCH` pattern filter, so one literal filter on
/// the topic name would miss a `*` ACL and every prefixed ACL. The three
/// filters are topic filters only, and never the whole cluster's ACL set.
#[test]
pub(crate) fn the_wal_topic_acl_filters_cover_the_topic_the_wildcard_and_every_prefix() {
    let filters = wal_topic_acl_filters("logs-wal");
    check!(filters.len() == 3);
    check!(filters[0].resource_type == Some(ResourceType::Topic));
    check!(filters[0].resource_name.as_deref() == Some("logs-wal"));
    check!(filters[0].pattern_type == Some(PatternType::Literal));
    check!(filters[1].resource_name.as_deref() == Some("*"));
    check!(filters[1].pattern_type == Some(PatternType::Literal));
    check!(filters[2].resource_name.is_none());
    check!(filters[2].pattern_type == Some(PatternType::Prefixed));
}

/// Every row is one lookup state at one age, under a ten-second TTL and a
/// one-minute staleness bound. A fresh snapshot is served. An old one is
/// refreshed. After a failed refresh the broker is not asked again for one
/// TTL: the snapshot is served while it is within the bound, and without one
/// the check fails closed.
#[test]
pub(crate) fn a_cached_lookup_serves_refreshes_or_fails_closed_by_its_age() {
    let policy = policy_for_test(8);
    let start = Instant::now();
    let at = |seconds| start + Duration::from_secs(seconds);
    let value = Arc::new(7_u8);
    let lookup = |fetched: Option<u64>, failed: Option<u64>| CachedLookup {
        snapshot: fetched.map(|seconds| CachedSnapshot {
            value: Arc::clone(&value),
            fetched_at: at(seconds),
        }),
        last_failure: failed.map(|seconds| CachedFailure {
            failed_at: at(seconds),
            reason: "down".to_string(),
        }),
    };
    let serve = || CachedLookupDecision::Serve(Arc::clone(&value));
    let unavailable = || CachedLookupDecision::Unavailable("down".to_string());

    struct CacheLookupCase {
        name: &'static str,
        lookup: CachedLookup<u8>,
        seconds: u64,
        expected: CachedLookupDecision<u8>,
    }
    let cases = [
        CacheLookupCase {
            name: "no snapshot",
            lookup: lookup(None, None),
            seconds: 0,
            expected: CachedLookupDecision::Refresh,
        },
        CacheLookupCase {
            name: "a fresh snapshot",
            lookup: lookup(Some(0), None),
            seconds: 9,
            expected: serve(),
        },
        CacheLookupCase {
            name: "a snapshot one TTL old",
            lookup: lookup(Some(0), None),
            seconds: 10,
            expected: CachedLookupDecision::Refresh,
        },
        CacheLookupCase {
            name: "a recent failure over a stale snapshot",
            lookup: lookup(Some(0), Some(30)),
            seconds: 35,
            expected: serve(),
        },
        CacheLookupCase {
            name: "a recent failure at the bound",
            lookup: lookup(Some(0), Some(55)),
            seconds: 60,
            expected: serve(),
        },
        CacheLookupCase {
            name: "a recent failure past the bound",
            lookup: lookup(Some(0), Some(55)),
            seconds: 61,
            expected: unavailable(),
        },
        CacheLookupCase {
            name: "a recent failure with no snapshot",
            lookup: lookup(None, Some(0)),
            seconds: 9,
            expected: unavailable(),
        },
        CacheLookupCase {
            name: "an old failure with no snapshot",
            lookup: lookup(None, Some(0)),
            seconds: 10,
            expected: CachedLookupDecision::Refresh,
        },
        CacheLookupCase {
            name: "an old failure over a stale snapshot",
            lookup: lookup(Some(0), Some(30)),
            seconds: 40,
            expected: CachedLookupDecision::Refresh,
        },
    ];

    for case in cases {
        check!(
            case.lookup.decide(at(case.seconds), &policy) == case.expected,
            "{}",
            case.name
        );
    }

    let mut lookup = lookup(Some(0), None);
    check!(lookup.fail("down".to_string(), at(60), &policy) == Ok(Arc::clone(&value)));
    check!(lookup.fail("down".to_string(), at(61), &policy) == Err("down".to_string()));
    check!(lookup.store(9, at(62)) == Arc::new(9));
    check!(lookup.last_failure.is_none());
}

/// A broker that stops answering does not stop authorization at once. The
/// last good ACLs are served, and `connected` reads `false`, until the
/// snapshot is older than the staleness bound. From then on, the check fails
/// closed.
#[tokio::test]
pub(crate) async fn a_failed_refresh_keeps_serving_the_last_snapshot_until_the_staleness_bound() {
    let tenant = tenant_for_test("tenant-a");
    let source = security_disabled_broker_access();
    let fixture = broker_access_for_test(&source, 8);
    let access = fixture.cache;
    let connected = fixture.connected;
    let clock = fixture.clock;
    let authorizer = BrokerBackedQueryAuthorizer { access };
    check!(
        authorizer
            .check(&Principal::Unauthenticated, &tenant)
            .await
            .is_ok()
    );
    check!(connected.load(AtomicOrdering::SeqCst));

    *source.acls.lock().expect("fake ACL lock") = Err("broker unreachable".to_string());
    clock.advance(secs(11));
    check!(authorizer.access.refresh_acls().await.is_ok());
    check!(!connected.load(AtomicOrdering::SeqCst));
    check!(
        authorizer
            .check(&Principal::Unauthenticated, &tenant)
            .await
            .is_ok()
    );

    // 59 seconds old: still within the one-minute bound.
    clock.advance(secs(48));
    check!(
        authorizer
            .check(&Principal::Unauthenticated, &tenant)
            .await
            .is_ok()
    );

    // 61 seconds old: past the bound, so the check fails closed.
    clock.advance(secs(2));
    check!(matches!(
        authorizer.check(&Principal::Unauthenticated, &tenant).await,
        Err(QueryAuthorizationError::Unavailable { tenant, reason })
            if tenant == "tenant-a" && reason == "broker unreachable"
    ));

    // The broker answers again, and the next lookup serves the new snapshot.
    // It grants another tenant only, so it refuses `tenant-a`.
    *source.acls.lock().expect("fake ACL lock") = Ok(AclSet::Configured(vec![AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "__krabka_".to_string(),
        pattern_type: PatternType::Prefixed,
        principal: "User:tenant-b".to_string(),
        host: "*".to_string(),
        operation: AclOperation::All,
        permission_type: PermissionType::Allow,
    }]));
    clock.advance(secs(10));
    check!(matches!(
        authorizer.check(&Principal::Unauthenticated, &tenant).await,
        Err(QueryAuthorizationError::Unauthorized { .. })
    ));
    check!(connected.load(AtomicOrdering::SeqCst));
}

/// The refresh task asks the broker at its start and then once per TTL, so a
/// request finds a fresh snapshot without asking itself. A cancel stops it.
///
/// The task sleeps on the runtime's timer, so this test runs on real time with
/// a TTL of 20 milliseconds and checks counts with a margin.
#[tokio::test]
pub(crate) async fn keep_fresh_asks_the_broker_once_per_ttl_until_cancelled() {
    let source = Arc::new(CountingBrokerAccess::answering(Ok(
        AclSet::SecurityDisabled,
    )));
    let connected = Arc::new(AtomicBool::new(false));
    let access = BrokerAccessCache::new(
        Arc::clone(&source) as Arc<dyn BrokerAccessSource>,
        "__krabka_observability_logs_wal".to_string(),
        BrokerAccessPolicy {
            ttl: millis(20),
            max_staleness: secs(60),
            tenant_capacity: NonZeroUsize::new(8).expect("a nonzero capacity"),
        },
        Arc::clone(&connected),
    );
    let limiter = Arc::new(BrokerBackedIngestLimiter::with_access(access, secs(1)));
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let limiter = Arc::clone(&limiter);
        let token = token.clone();
        async move { limiter.keep_fresh(token).await }
    });

    tokio::time::sleep(millis(110).to_std()).await;
    let refreshes = source.lookups().acl;
    check!((3..=7).contains(&refreshes), "{refreshes} refreshes");
    check!(connected.load(AtomicOrdering::SeqCst));

    token.cancel();
    check!(task.await.is_ok());
    let stopped_at = source.lookups().acl;
    tokio::time::sleep(millis(60).to_std()).await;
    check!(source.lookups().acl == stopped_at);
}

/// With no snapshot, an unreachable broker refuses the check. The failure is
/// kept for one TTL, so the requests after it are refused from memory rather
/// than waiting in turn on a broker that just failed.
#[tokio::test]
pub(crate) async fn no_snapshot_and_an_unreachable_broker_fail_closed_without_a_queue() {
    let tenant = tenant_for_test("tenant-a");

    let source = Arc::new(CountingBrokerAccess::answering(Err(
        "broker unreachable".to_string()
    )));
    let fixture = broker_access_for_test(&source, 8);
    let access = fixture.cache;
    let connected = fixture.connected;
    let authorizer = BrokerBackedQueryAuthorizer { access };
    for _ in 0..10 {
        check!(matches!(
            authorizer.check(&Principal::Unauthenticated, &tenant).await,
            Err(QueryAuthorizationError::Unavailable { reason, .. }) if reason == "broker unreachable"
        ));
    }
    check!(source.lookups() == BrokerLookups { acl: 1, quota: 0 });
    check!(!connected.load(AtomicOrdering::SeqCst));

    let source = Arc::new(CountingBrokerAccess::answering(Ok(
        AclSet::SecurityDisabled,
    )));
    *source.quotas.lock().expect("fake quota lock") = Err("broker unreachable".to_string());
    let fixture = broker_access_for_test(&source, 8);
    let access = fixture.cache;
    let clock = fixture.clock;
    let limiter = BrokerBackedIngestLimiter::with_access(access, secs(1));
    for _ in 0..10 {
        check!(matches!(
            limiter.check(&Principal::Unauthenticated, &tenant, &[]).await,
            Err(IngestLimitError::Unavailable { reason, .. }) if reason == "broker unreachable"
        ));
    }
    check!(source.lookups() == BrokerLookups { acl: 1, quota: 1 });

    // One TTL later the broker is asked again.
    clock.advance(secs(10));
    check!(
        limiter
            .check(&Principal::Unauthenticated, &tenant, &[])
            .await
            .is_err()
    );
    check!(source.lookups() == BrokerLookups { acl: 2, quota: 2 });
}

/// A query and a push each read the broker's answers from memory. Inside one
/// TTL, a hundred checks for one tenant cost one ACL lookup and one quota
/// lookup, not a hundred of each on a shared admin connection.
#[tokio::test]
pub(crate) async fn repeated_checks_inside_the_ttl_ask_the_broker_once() {
    let tenant = tenant_for_test("tenant-a");

    let source = security_disabled_broker_access();
    let fixture = broker_access_for_test(&source, 8);
    let access = fixture.cache;
    let connected = fixture.connected;
    let authorizer = BrokerBackedQueryAuthorizer { access };
    for _ in 0..100 {
        check!(
            authorizer
                .check(&Principal::Unauthenticated, &tenant)
                .await
                .is_ok()
        );
    }
    check!(source.lookups() == BrokerLookups { acl: 1, quota: 0 });
    check!(connected.load(AtomicOrdering::SeqCst));

    let source = security_disabled_broker_access();
    let fixture = broker_access_for_test(&source, 8);
    let access = fixture.cache;
    let clock = fixture.clock;
    let limiter = BrokerBackedIngestLimiter::with_access(access, secs(1));
    for _ in 0..100 {
        check!(
            limiter
                .check(&Principal::Unauthenticated, &tenant, &[])
                .await
                .is_ok()
        );
    }
    check!(source.lookups() == BrokerLookups { acl: 1, quota: 1 });

    // One TTL later, the next check asks again, once.
    clock.advance(secs(10));
    check!(
        limiter
            .check(&Principal::Unauthenticated, &tenant, &[])
            .await
            .is_ok()
    );
    check!(
        limiter
            .check(&Principal::Unauthenticated, &tenant, &[])
            .await
            .is_ok()
    );
    check!(source.lookups() == BrokerLookups { acl: 2, quota: 2 });
}

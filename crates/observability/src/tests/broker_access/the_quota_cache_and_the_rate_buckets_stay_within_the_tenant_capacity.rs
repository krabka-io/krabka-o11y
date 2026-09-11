use super::*;

fn record_for_test(tenant: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: tenant.to_string(),
        labels: BTreeMap::from([("app".to_string(), "api".to_string())]),
        timestamp_ns: 1,
        line: "line".to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

/// A client can name any number of tenants. The quota snapshots and the rate
/// buckets hold at most the configured count, so a spray of tenant names
/// cannot grow the distributor's memory.
#[tokio::test]
pub(crate) async fn the_quota_cache_and_the_rate_buckets_stay_within_the_tenant_capacity() {
    let source = Arc::new(CountingBrokerAccess::answering(Ok(
        AclSet::SecurityDisabled,
    )));
    *source.quotas.lock().expect("fake quota lock") = Ok(BTreeMap::from([(
        "producer_byte_rate".to_string(),
        1_000_000.0,
    )]));
    let (access, _, _) = broker_access_for_test(&source, 3);
    let limiter = BrokerBackedIngestLimiter::with_access(access, secs(1));

    for index in 0..50 {
        let name = format!("tenant-{index}");
        let tenant = tenant_for_test(&name);
        check!(
            limiter
                .check(
                    &Principal::Unauthenticated,
                    &tenant,
                    &[record_for_test(&name)]
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
    check!(source.lookups() == (1, 50));
}

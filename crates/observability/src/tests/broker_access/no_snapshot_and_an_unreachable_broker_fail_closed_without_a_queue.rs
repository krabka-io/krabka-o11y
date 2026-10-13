use super::*;

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

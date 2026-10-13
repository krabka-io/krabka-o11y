use super::*;

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

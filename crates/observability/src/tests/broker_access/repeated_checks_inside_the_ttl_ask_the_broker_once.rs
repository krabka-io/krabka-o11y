use super::*;

/// A query and a push each read the broker's answers from memory. Inside one
/// TTL, a hundred checks for one tenant cost one ACL lookup and one quota
/// lookup, not a hundred of each on a shared admin connection.
#[tokio::test]
pub(crate) async fn repeated_checks_inside_the_ttl_ask_the_broker_once() {
    let tenant = tenant_for_test("tenant-a");

    let source = Arc::new(CountingBrokerAccess::answering(Ok(
        AclSet::SecurityDisabled,
    )));
    let (access, connected, _) = broker_access_for_test(&source, 8);
    let authorizer = BrokerBackedQueryAuthorizer { access };
    for _ in 0..100 {
        check!(
            authorizer
                .check(&Principal::Unauthenticated, &tenant)
                .await
                .is_ok()
        );
    }
    check!(source.lookups() == (1, 0));
    check!(connected.load(AtomicOrdering::SeqCst));

    let source = Arc::new(CountingBrokerAccess::answering(Ok(
        AclSet::SecurityDisabled,
    )));
    let (access, _, clock) = broker_access_for_test(&source, 8);
    let limiter = BrokerBackedIngestLimiter::with_access(access, secs(1));
    for _ in 0..100 {
        check!(
            limiter
                .check(&Principal::Unauthenticated, &tenant, &[])
                .await
                .is_ok()
        );
    }
    check!(source.lookups() == (1, 1));

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
    check!(source.lookups() == (2, 2));
}

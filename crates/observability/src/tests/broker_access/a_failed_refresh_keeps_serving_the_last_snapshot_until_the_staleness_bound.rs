use super::*;

/// A broker that stops answering does not stop authorization at once. The
/// last good ACLs are served, and `connected` reads `false`, until the
/// snapshot is older than the staleness bound. From then on, the check fails
/// closed.
#[tokio::test]
pub(crate) async fn a_failed_refresh_keeps_serving_the_last_snapshot_until_the_staleness_bound() {
    let tenant = tenant_for_test("tenant-a");
    let source = Arc::new(CountingBrokerAccess::answering(Ok(
        AclSet::SecurityDisabled,
    )));
    let (access, connected, clock) = broker_access_for_test(&source, 8);
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

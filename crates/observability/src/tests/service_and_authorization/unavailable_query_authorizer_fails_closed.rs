use super::*;

#[tokio::test]
pub(crate) async fn unavailable_query_authorizer_fails_closed() {
    let result = UnavailableQueryAuthorizer
        .check(
            &Principal::Unauthenticated,
            &TenantId::new("tenant-a").expect("a valid tenant id"),
        )
        .await;

    assert2::assert!(matches!(
        result,
        Err(QueryAuthorizationError::Unavailable { tenant, .. }) if tenant == "tenant-a"
    ));
}

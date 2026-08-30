use super::*;

#[tokio::test]
pub(crate) async fn unavailable_query_authorizer_fails_closed() {
    let result = UnavailableQueryAuthorizer.check("tenant-a").await;

    assert2::assert!(matches!(
        result,
        Err(QueryAuthorizationError::Unavailable { tenant, .. }) if tenant == "tenant-a"
    ));
}

use super::*;

/// A single authorized tenant takes the per-tenant path, and that is the
/// only path applying `max_query_range` and the query-length limit -- the
/// multi-tenant path's scalar shortcut checks neither. Routing one tenant
/// through it would serve a query those limits refuse.
#[tokio::test]
pub(crate) async fn a_single_tenant_query_still_meets_the_configured_range_limit() {
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_max_query_range(Time::from_nanos(1_000_000_000));
    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
    let params = QueryParams {
        query: "vector(1)".to_owned(),
        time: None,
        start: Some(0),
        end: Some(10_000_000_000),
        since: None,
        step: Some(1_000_000_000),
        interval: None,
        limit: None,
        direction: None,
        delay_for: None,
    };

    let error = execute_http_query(&state, &headers, params, QueryKind::Range)
        .await
        .expect_err("ten seconds is past the one-second maximum");
    check!(matches!(error, HttpQueryError::QueryRangeTooLarge { .. }));
}

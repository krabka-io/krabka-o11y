use super::*;

#[tokio::test]
pub(crate) async fn read_paths_admit_requests_when_limits_are_off() {
    let uris = [
        "/api/v1/query?query=count(up)&time=0",
        "/api/v1/query_range?query=count(up)&start=0&end=60&step=60",
        "/api/v1/labels?start=0&end=1",
        "/api/v1/label/job/values?start=0&end=1",
        "/api/v1/series?match[]=up&start=0&end=1",
        "/api/v1/query_exemplars?query=up&start=0&end=1",
    ];
    // Every cap turned off through a zero sentinel admits the request. So do the
    // default limits that a state without `with_query_limits` applies, because
    // an ordinary request is far inside them.
    let off = Limits {
        max_samples_per_query: 0,
        max_fetched_series_per_query: 0,
        max_query_lookback: minutes(0),
        max_query_length: minutes(0),
        ..Limits::default()
    };

    for uri in uris {
        for limits in [Some(off.clone()), None] {
            let state =
                PrometheusApiState::new(Arc::new(two_series_store()), EngineOpts::default());
            let state = match limits {
                Some(limits) => state.with_query_limits(OverridesProvider::new(limits)),
                None => state,
            };

            let response = prometheus_router(Arc::new(state))
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .header("x-scope-orgid", "tenant-a")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert2::assert!(response.status() == StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert2::assert!(body["status"].as_str() == Some("success"), "{uri}");
        }
    }
}

use krabka_blockstore::{ERASURE_REQUEST_PREFIX, Labels, list_erasure_requests};
use object_store::memory::InMemory;
use tower::ServiceExt as _;

use super::*;

#[tokio::test]
async fn prometheus_admin_routes_persist_tombstones_until_compaction() {
    let mut metric_store = InMemoryMetricStore::new();
    metric_store.push_float(
        "tenant-a",
        Labels::from_pairs([("__name__", "requests_total"), ("user", "7")]),
        1_000,
        1.0,
    );
    let erasure_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(metric_store), EngineOpts::default())
            .with_erasure_store(Arc::clone(&erasure_store)),
    );
    let app = prometheus_router(state);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/api/v1/admin/tsdb/delete_series")
                .header("x-scope-orgid", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(axum::body::Body::from(
                    "match%5B%5D=%7Buser%3D%227%22%7D&start=1&end=1",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    check!(response.status() == StatusCode::NO_CONTENT);
    let requests = list_erasure_requests(&erasure_store, ERASURE_REQUEST_PREFIX)
        .await
        .unwrap();
    check!(requests.len() == 1);
    check!(requests[0].tenant == "tenant-a");
    check!(requests[0].selector == "{user=\"7\"}");
    check!(requests[0].matcher_sets.len() == 1);

    let response = app
        .oneshot(
            axum::http::Request::post("/api/v1/admin/tsdb/clean_tombstones")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    check!(response.status() == StatusCode::NO_CONTENT);
    let requests = list_erasure_requests(&erasure_store, ERASURE_REQUEST_PREFIX)
        .await
        .unwrap();
    check!(requests.len() == 1);
    check!(requests[0].clean_requested);
}

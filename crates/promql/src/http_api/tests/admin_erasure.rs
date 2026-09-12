use krabka_blockstore::{ERASURE_REQUEST_PREFIX, Labels, list_erasure_requests};
use krabka_observability::server_security::{ClientAuth, ServerSecurityArgs};
use krabka_query_frontend::{CacheKey, ExecutionOptions, QueryCache};
use object_store::memory::InMemory;
use tower::ServiceExt as _;

use super::*;
use crate::AnnotatedQueryResult;

struct UnusableCache;

#[async_trait::async_trait]
impl QueryCache<AnnotatedQueryResult> for UnusableCache {
    type Error = PromqlError;

    async fn get(&self, _key: &CacheKey) -> Result<Option<AnnotatedQueryResult>, Self::Error> {
        panic!("erasure-active query used the frontend cache")
    }

    async fn insert(
        &self,
        _key: &CacheKey,
        _result: &AnnotatedQueryResult,
    ) -> Result<(), Self::Error> {
        panic!("erasure-active query used the frontend cache")
    }
}

impl RangeQueryCache for UnusableCache {
    fn execution_options(&self) -> ExecutionOptions {
        ExecutionOptions::default()
    }
}

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
            .with_erasure_store(Arc::clone(&erasure_store))
            .with_query_frontend_cache(
                QueryFrontendOptions {
                    split_interval: secs(120),
                    shard_count: 1,
                },
                Arc::new(UnusableCache),
            ),
    );
    let app = prometheus_router(Arc::clone(&state));

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

    let (status, _) = annotated_query_body(
        Arc::clone(&state),
        "/api/v1/query_range?query=requests_total&start=1&end=1&step=1",
    )
    .await;
    check!(status == StatusCode::OK);

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
}

#[tokio::test]
async fn delete_series_validates_every_selector_before_persisting() {
    let erasure_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_erasure_store(Arc::clone(&erasure_store)),
    );
    let response = prometheus_router(state)
        .oneshot(
            axum::http::Request::post("/api/v1/admin/tsdb/delete_series")
                .header("x-scope-orgid", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(axum::body::Body::from(
                    "match%5B%5D=%7Buser%3D%227%22%7D&match%5B%5D=%7Bbroken",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::BAD_REQUEST);
    check!(
        list_erasure_requests(&erasure_store, ERASURE_REQUEST_PREFIX)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn delete_series_requires_an_admin_principal() {
    let directory = tempfile::tempdir().unwrap();
    let credentials = directory.path().join("credentials.yaml");
    std::fs::write(
        &credentials,
        "principals:\n  - name: reader\n    token_sha256: [\"fd66d679ee99eef3efa2ef4127680aea063ca35c6b394cacad365fdeec1203be\"]\n    tenants: [\"tenant-a\"]\n",
    )
    .unwrap();
    let security = ServerSecurityArgs {
        server_tls_cert_path: None,
        server_tls_key_path: None,
        server_tls_client_ca_path: None,
        server_tls_client_auth: ClientAuth::NoClientCert,
        server_tls_handshake_timeout: secs(10),
        auth_credentials_config: Some(credentials),
        internal_client_token_path: None,
        internal_client_tls_cert_path: None,
        internal_client_tls_key_path: None,
        internal_client_tls_ca_path: None,
    }
    .load()
    .unwrap();
    let erasure_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_erasure_store(Arc::clone(&erasure_store)),
    );
    let app = authenticate_requests(super::super::prometheus_router(state), &security);
    let response = app
        .oneshot(
            axum::http::Request::post("/api/v1/admin/tsdb/delete_series")
                .header(
                    "authorization",
                    "Bearer ops-token-5f1c9d2e7a4b8c3d6e0f1a2b3c4d5e6f",
                )
                .header("x-scope-orgid", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(axum::body::Body::from("match%5B%5D=%7Buser%3D%227%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    check!(response.status() == StatusCode::FORBIDDEN);
    check!(
        list_erasure_requests(&erasure_store, ERASURE_REQUEST_PREFIX)
            .await
            .unwrap()
            .is_empty()
    );
}

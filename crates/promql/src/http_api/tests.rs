use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use assert2::check;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_blockstore::Labels;
use krabka_metrics::{Limits, NativeHistogram, OverridesProvider, ResetHint};
use krabka_observability::server_security::{
    ClientAuth, ServerSecurity, ServerSecurityArgs, authenticate_requests,
};
use tower::ServiceExt;

use super::{request::unix_now_ms, *};
use crate::{InMemoryMetricStore, PromqlMatcher as LabelMatcher, ScanResult};

// Every request reaches the handlers through the authentication layer, as it
// does on a served listener. With no credentials file, the layer marks each
// request unauthenticated and lets it through.
fn prometheus_router<S: MetricStore + 'static>(state: Arc<PrometheusApiState<S>>) -> Router {
    authenticate_requests(super::prometheus_router(state), &ServerSecurity::default())
}

fn server_security_args(credentials: std::path::PathBuf) -> ServerSecurityArgs {
    ServerSecurityArgs {
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
}

mod admin_erasure;
mod annotated_query_body;
mod limited_get;
mod mimir_alertmanager_api;
mod query_reports_sample_cap_as_execution_error;
mod query_timeout_and_stats;
mod ruler_filters_and_pagination;
mod search_and_native_histogram_cardinality;
mod slow_empty_store;
mod tenant_limits;

use annotated_query_body::{OrgRequest, annotated_query_body, org_query_body};
mod query_annotations;
use annotation_query_uri::annotation_query_uri;
use annotation_store::annotation_store;
use limited_get::{assert_execution_error, limited_get};
use slow_empty_store::SlowEmptyStore;
use two_series_store::two_series_store;

async fn assert_series_limit_rejected(uri: &str) {
    let limits = Limits {
        max_fetched_series_per_query: 1,
        ..Limits::default()
    };
    let response = limited_get(two_series_store(), limits, uri).await;
    assert_execution_error(
        &response,
        |error| error == "series per query exceeded: observed 2 above limit 1",
        uri,
    );
}

mod query_semantics;

mod ruler_api_cases;

mod http_semantics;

mod support;
pub(crate) use support::{
    annotation_query_uri, annotation_store, first_vector_sample, two_series_store,
};

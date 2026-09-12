//! The query path stays open after the broker-backed authorizer connects.
//!
//! The querier serves queries through an authorizer slot that starts out
//! unavailable and is swapped for a broker-backed authorizer by a background
//! connect task. Every query takes a read lock on that slot, so the swap must
//! not leave a writer parked on it. Holding that write guard across the task's
//! shutdown await wedged every later query for the life of the service, and a
//! wedged query never returns rather than failing. One bad service then became
//! an unbounded CI stall.
//!
//! `unavailable_query_authorizer_fails_closed` and
//! `a_role_with_a_broker_fails_closed_on_rules_and_deletes_until_its_authorizer_connects`
//! both check the half before the swap. This checks the half after it, and it
//! bounds every request: a wedge fails the test instead of hanging the run.
//!
//! The broker runs in this process, so this is an ordinary `bazel test`
//! target and needs no container.

mod support;

use std::{
    collections::BTreeMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_observability::{
    QuerierIndexSource, Role, ServiceConfig, ServiceDependencies, build_service_dependencies,
    build_service_router, run_compactor_until_idle, wal_consumer_metrics::WalConsumerMetrics,
};
use krabka_units::secs;
use serde_json::{Value, json};
use support::test_service_config;
use tempfile::TempDir;
use tower::ServiceExt as _;

/// The tenant the seed push and every query name.
const TENANT: &str = "tenant-a";

/// Where the block builder writes blocks and shard indexes.
const INDEX_PREFIX: &str = "observability/logs";

/// The WAL topic the three roles share.
const WAL_TOPIC: &str = "__krabka_observability_logs_wal_authorization";

/// The bound on one request.
///
/// A wedged query path never returns, so an unbounded request here would hang
/// the test run instead of failing it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to keep retrying while the background authorizer connect lands.
const CONNECT_DEADLINE: Duration = Duration::from_secs(60);

/// The two queries that take the authorizer read path.
///
/// A metric query reaches the same slot before any range-vector evaluation
/// runs, so it wedges on exactly the same lock as a stream query and is worth
/// asking separately.
const QUERIES: &[(&str, &str)] = &[
    ("stream", r#"{app="api",env="prod"}"#),
    ("metric", r#"count_over_time({app="api",env="prod"} [2s])"#),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_querier_serves_queries_after_its_authorizer_connects() {
    let stack = Stack::boot().await;

    for (kind, query) in QUERIES {
        let deadline = Instant::now() + CONNECT_DEADLINE;
        let mut last = None;
        let mut served = false;
        while Instant::now() < deadline {
            let status = stack.query_status(query).await;
            // `None` is the regression: the request never came back at all.
            assert!(
                status.is_some(),
                "the {kind} query never returned within {REQUEST_TIMEOUT:?}: the querier's \
                 authorizer slot is wedged"
            );
            last = status;
            if status == Some(StatusCode::OK) {
                served = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        check!(
            served,
            "the {kind} query was never served; last status {last:?}"
        );
    }

    stack.shutdown().await;
}

/// A broker, a seeded block, and the querier that reads it.
struct Stack {
    broker: BrokerHandle,
    querier: Router,
    start_ns: i64,
    end_ns: i64,
    _dirs: Vec<TempDir>,
}

impl Stack {
    async fn boot() -> Self {
        let broker_dir = TempDir::new().expect("broker tempdir");
        let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        create_wal_topic(&bootstrap).await;

        let data_root = TempDir::new().expect("data root");
        let object_root = TempDir::new().expect("object root");
        let object_store_url = format!("file://{}", object_root.path().display());

        let start_ns = current_unix_second_ns() - 60_000_000_000;
        let end_ns = start_ns + 2_000_000_000;

        let distributor = router(&config(Role::Distributor, &bootstrap, &data_root)).await;
        let response = distributor
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/loki/api/v1/push")
                    .header("content-type", "application/json")
                    .header("X-Scope-OrgID", TENANT)
                    .body(Body::from(push_body(start_ns).to_string()))
                    .expect("push request"),
            )
            .await
            .expect("push response");
        assert!(response.status() == StatusCode::NO_CONTENT);

        let mut block_builder = config(Role::BlockBuilder, &bootstrap, &data_root);
        block_builder.object_store_url = Some(object_store_url.clone());
        block_builder.index_prefix = Some(INDEX_PREFIX.to_string());
        block_builder.wal_group_id = "krabka-observability-authorization-block-builder".to_string();
        let descriptors =
            run_compactor_until_idle(&block_builder, dependencies(&block_builder).await, None)
                .await
                .expect("block builder run");
        assert!(!descriptors.is_empty());

        let mut querier_config = config(Role::Querier, &bootstrap, &data_root);
        querier_config.object_store_url = Some(object_store_url);
        querier_config.index_prefix = Some(INDEX_PREFIX.to_string());
        querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
        querier_config.wal_group_id = "krabka-observability-authorization-querier".to_string();

        Self {
            broker,
            querier: router(&querier_config).await,
            start_ns,
            end_ns,
            _dirs: vec![broker_dir, data_root, object_root],
        }
    }

    /// Issues one query, and answers `None` when it did not return within
    /// [`REQUEST_TIMEOUT`], that is when the query path is wedged.
    async fn query_status(&self, query: &str) -> Option<StatusCode> {
        let uri = format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}&step=1s",
            percent_encode(query),
            self.start_ns,
            self.end_ns,
        );
        let request = Request::builder()
            .uri(uri)
            .header("X-Scope-OrgID", TENANT)
            .body(Body::empty())
            .expect("query request");
        tokio::time::timeout(REQUEST_TIMEOUT, self.querier.clone().oneshot(request))
            .await
            .ok()
            .map(|response| response.expect("query response").status())
    }

    async fn shutdown(self) {
        self.broker.shutdown().await;
    }
}

fn config(target: Role, bootstrap: &str, data_root: &TempDir) -> ServiceConfig {
    ServiceConfig {
        wal_bootstrap_server: Some(bootstrap.to_string()),
        wal_topic: WAL_TOPIC.to_string(),
        wal_group_id: format!("krabka-observability-authorization-{target:?}"),
        ..test_service_config(target, data_root.path().to_path_buf())
    }
}

async fn dependencies(config: &ServiceConfig) -> ServiceDependencies {
    build_service_dependencies(config, WalConsumerMetrics::unregistered())
        .await
        .expect("service dependencies")
}

async fn router(config: &ServiceConfig) -> Router {
    build_service_router(config, dependencies(config).await, None)
        .await
        .expect("role router")
}

async fn create_wal_topic(bootstrap: &str) {
    let servers = [bootstrap.to_string()];
    let mut admin = AdminClient::connect(&servers).await.expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: WAL_TOPIC.to_string(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            secs(10),
        )
        .await
        .expect("create the wal topic");
}

fn push_body(start_ns: i64) -> Value {
    json!({
        "streams": [{
            "stream": { "app": "api", "env": "prod" },
            "values": [
                [start_ns.to_string(), "aa"],
                [(start_ns + 1_000_000_000).to_string(), "bbb"],
            ],
        }],
    })
}

/// Percent-encodes one query-string component.
///
/// A `LogQL` selector carries `{`, `"`, `=` and spaces, and an unencoded one
/// reaches the router as a different query than the one asked.
fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn current_unix_second_ns() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the unix epoch")
        .as_secs();
    i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000
}

//! `--target all`: the whole logs path in one process, and a stop that does
//! not lose the last push.
//!
//! The single-role suites each drive one stage with the other two stubbed out,
//! so none of them could have caught the failure this target is most likely to
//! have: three roles that start, bind, and quietly do not reach each other --
//! a block builder writing into one object store while the querier reads
//! another, or a querier tailing the same consumer group as the block builder
//! and so seeing half the WAL. Both of those pass a readiness probe.
//! Neither passes this suite, because it pushes at the ingest route and reads
//! back at the query route, against a real broker, in one process.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_blockstore::{BlockKey, TimeRange, read_log_block_from_object_store};
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_admin::{
    AclEntry, AclOperation, AdminClient, CreateTopicSpec, PatternType, PermissionType, ResourceType,
};
use krabka_observability::{
    CancellationToken, QuerierIndexSource, Role, ServiceConfig, build_service_dependencies,
    serve_all_service_listener, wal_consumer_metrics::WalConsumerMetrics,
};
use krabka_units::{days, secs};
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
use serde_json::{Value, json};

const TENANT: &str = "tenant-a";
const INDEX_PREFIX: &str = "observability/logs";

/// How long a step that waits on the broker and the block builder may take.
///
/// A produce is acknowledged before a fresh consumer group has been assigned
/// its partition, and the block builder polls on an interval, so an early
/// query legitimately answers with nothing. That is a retry, not a failure;
/// this bounds the retrying.
const DEADLINE: Duration = Duration::from_secs(45);

/// Push at the ingest route, read back at the query route, one process.
///
/// The line has to survive the distributor, the broker, the block builder, the
/// object store and the querier without any of those being a test double, and
/// the two routes are different APIs on the one port `--target all` binds --
/// which is the port a `Loki` datasource would be pointed at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_line_pushed_into_the_all_in_one_comes_back_out_of_its_query_api() {
    let stack = AllInOne::start().await;

    let pushed = stack.push(push_body()).await;
    check!(pushed == reqwest::StatusCode::NO_CONTENT);

    let result = stack.query_until_answered().await;

    check!(
        result
            == json!([{
                "stream": {
                    "app": "api",
                    "detected_level": "error",
                    "env": "prod",
                    "service_name": "api",
                    "trace_id": "abc",
                },
                "values": [["10", "api error"]],
            }])
    );

    let _object_dir = stack.stop().await;
}

/// A drain is one request, and it has to move one gate.
///
/// Three roles register their preconditions on one readiness, and two places
/// in the all-in-one want the distributor's drain gate: the router, which
/// serves `POST /ingester/prepare_shutdown`, and the stop, whose first step
/// clears it. Registered twice, `/ready` would name
/// `distributor/accepting-writes` twice and the operator's request would clear
/// only one of them -- so the probe would go on answering 200 through a drain
/// that had been asked for, and the load balancer would go on sending pushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_request_takes_the_all_in_one_out_of_rotation() {
    let stack = AllInOne::start().await;
    check!(!stack.ready_body().await.1.contains("accepting-writes"));

    let drained = stack.prepare_shutdown().await;
    check!(drained == reqwest::StatusCode::NO_CONTENT);

    let (status, body) = stack.ready_body().await;
    check!(status == reqwest::StatusCode::SERVICE_UNAVAILABLE);
    // Once, not twice. The querier's own gates flap as its hot tail
    // reconnects and disconnects, so the rest of the line is not this test's
    // business; what is its business is that one drain request moved one gate.
    check!(
        body.matches("distributor/accepting-writes").count() == 1,
        "{body}"
    );

    let _object_dir = stack.stop().await;
}

/// What the ordered stop is supposed to leave behind.
///
/// A push acknowledged shortly before the stop is in the WAL and may be in no
/// block yet. If the block builder stopped first, or stopped alongside the
/// data port, that record would sit in the WAL waiting for a restart that, in
/// a one-process stack on a laptop, never comes. The staged drain closes the
/// data port first and only then lets the block builder empty the WAL behind
/// it, so by the time the process returns the port is gone and the record is
/// in a block.
///
/// The first push is there to get the stack past its start: once its line
/// comes back out of the query API, the block builder has joined its group and
/// is committing. The second push is the one the stop has to save, and it is
/// pushed with nothing waiting behind it.
///
/// This asserts the effect, not the ordering itself: the block builder's
/// ordinary loop may well win the race and write the block before the stop is
/// asked for, and a test that tried to lose that race on purpose would be a
/// flaky test rather than a stronger one. Either way the line must be durable
/// when the process returns. The ordering is held directly by the
/// `StagedDrain` suite in `krabka_observability::supervision`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stop_closes_the_data_port_and_leaves_the_last_push_in_a_block() {
    let stack = AllInOne::start().await;
    check!(stack.push(push_body()).await == reqwest::StatusCode::NO_CONTENT);
    stack.query_until_answered().await;

    check!(stack.push(late_push_body()).await == reqwest::StatusCode::NO_CONTENT);

    let addr = stack.addr;
    let object_dir = stack.stop().await;

    check!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "the data port outlived the stop"
    );
    let lines = drained_lines(object_dir.path()).await;
    check!(lines == ["api error", "api recovered", "api stopping"]);
}

/// The whole stack, started the way the binary starts it.
struct AllInOne {
    /// Kept alive for the life of the test: dropping it stops the broker.
    _broker: BrokerHandle,
    /// Kept alive for the life of the test: dropping it removes the blocks.
    object_dir: tempfile::TempDir,
    _data_root: tempfile::TempDir,
    _broker_dir: tempfile::TempDir,
    addr: std::net::SocketAddr,
    shutdown: CancellationToken,
    served: tokio::task::JoinHandle<()>,
    client: reqwest::Client,
}

impl AllInOne {
    async fn start() -> Self {
        let broker_dir = tempfile::tempdir().expect("broker tempdir");
        let broker = Broker::start(BrokerConfig::for_tests(broker_dir.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        let wal_topic = ServiceConfig::default().wal_topic;
        create_wal_topic(&bootstrap, &wal_topic).await;
        grant_tenant_wal_access_for_test(&bootstrap, &wal_topic, TENANT).await;

        let object_dir = tempfile::tempdir().expect("object store tempdir");
        let data_root = tempfile::tempdir().expect("data root");
        let config = ServiceConfig {
            target: Role::All,
            listen_addr: "127.0.0.1:0".parse().expect("listen addr"),
            object_store_url: Some(format!("file://{}", object_dir.path().display())),
            wal_bootstrap_server: Some(bootstrap.clone()),
            wal_topic,
            wal_group_id: "krabka-observability-all-in-one".to_string(),
            data_root: data_root.path().to_path_buf(),
            querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
            index_prefix: Some(INDEX_PREFIX.to_string()),
            // The fixture's timestamps are fixed rather than current, so the
            // distributor would otherwise refuse them as too old.
            reject_old_samples_max_age: days(36_500),
            ..ServiceConfig::default()
        };

        let dependencies = build_service_dependencies(&config, WalConsumerMetrics::unregistered())
            .await
            .expect("all-in-one dependencies");
        let listener = tokio::net::TcpListener::bind(config.listen_addr)
            .await
            .expect("bind the all-in-one data port");
        let addr = listener.local_addr().expect("bound address");
        let shutdown = CancellationToken::new();
        let served = tokio::spawn({
            let config = config.clone();
            let shutdown = shutdown.clone();
            async move {
                // Boxed for the same reason the binary boxes it: the
                // all-in-one's start-up is several KB of future.
                Box::pin(serve_all_service_listener(
                    listener,
                    config,
                    dependencies,
                    None,
                    shutdown,
                ))
                .await
                .expect("the all-in-one serves and stops cleanly");
            }
        });

        Self {
            _broker: broker,
            object_dir,
            _data_root: data_root,
            _broker_dir: broker_dir,
            addr,
            shutdown,
            served,
            client: reqwest::Client::new(),
        }
    }

    async fn push(&self, body: Value) -> reqwest::StatusCode {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let response = self
                .client
                .post(format!("http://{}/loki/api/v1/push", self.addr))
                .header("content-type", "application/json")
                .header("X-Scope-OrgID", TENANT)
                .body(body.to_string())
                .send()
                .await;
            match response {
                Ok(response) => return response.status(),
                // The listener is bound before this call, but the broker-backed
                // limiter behind it may still be settling. Retry rather than
                // fail on the first connection refused.
                Err(error) => assert!(
                    Instant::now() < deadline,
                    "the all-in-one never accepted a push: {error}"
                ),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// `GET /ready` on the data port, status and body.
    async fn ready_body(&self) -> (reqwest::StatusCode, String) {
        let response = self
            .client
            .get(format!("http://{}/ready", self.addr))
            .send()
            .await
            .expect("the readiness probe answers");
        let status = response.status();
        (status, response.text().await.expect("a readiness body"))
    }

    /// The drain an operator's `preStop` hook sends.
    async fn prepare_shutdown(&self) -> reqwest::StatusCode {
        self.client
            .post(format!("http://{}/ingester/prepare_shutdown", self.addr))
            .send()
            .await
            .expect("the drain request is accepted")
            .status()
    }

    /// Queries until the block builder has made the line visible, or the
    /// deadline passes.
    async fn query_until_answered(&self) -> Value {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let response = self
                .client
                .get(format!(
                    "http://{}/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward",
                    self.addr
                ))
                .header("X-Scope-OrgID", TENANT)
                .send()
                .await;
            if let Ok(response) = response
                && response.status() == reqwest::StatusCode::OK
            {
                let body: Value = response.json().await.expect("query response is JSON");
                if body["data"]["result"]
                    .as_array()
                    .is_some_and(|r| !r.is_empty())
                {
                    check!(body["status"] == json!("success"));
                    check!(body["data"]["resultType"] == json!("streams"));
                    return body["data"]["result"].clone();
                }
            }
            assert!(
                Instant::now() < deadline,
                "the all-in-one never answered with the line it was pushed"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stops the process and hands back the object store it wrote to, so an
    /// assertion can read what the drain left there after the roles are gone.
    async fn stop(self) -> tempfile::TempDir {
        self.shutdown.cancel();
        tokio::time::timeout(DEADLINE, self.served)
            .await
            .expect("the all-in-one stops within the deadline")
            .expect("the all-in-one task did not panic");
        self.object_dir
    }
}

/// Every log line the object store holds under the index prefix, read back
/// the way another process would read it.
///
/// The blocks are found by walking the store rather than by naming a key,
/// because how the block builder divides a WAL into blocks is its business:
/// what this suite is entitled to assert is that the lines are durable once
/// the process has returned, not which file they landed in.
async fn drained_lines(root: &std::path::Path) -> Vec<String> {
    let store = LocalFileSystem::new_with_prefix(root).expect("object store");
    let mut lines = Vec::new();
    for key in block_keys(&root.join(INDEX_PREFIX)) {
        let rows = read_log_block_from_object_store(&store, &ObjectPath::from(INDEX_PREFIX), &key)
            .await
            .expect("a drained block is readable");
        lines.extend(rows.into_iter().map(|row| row.line));
    }
    lines.sort();
    lines
}

/// The block keys under `prefix`, rebuilt from the layout
/// [`BlockKey::object_key`] writes.
fn block_keys(prefix: &std::path::Path) -> Vec<BlockKey> {
    fn walk(dir: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                found.push(path);
            }
        }
    }

    let mut files = Vec::new();
    walk(prefix, &mut files);
    files.sort();
    files
        .iter()
        .filter_map(|path| {
            let relative = path.strip_prefix(prefix).ok()?;
            let parts: Vec<&str> = relative.to_str()?.split('/').collect();
            let [tenant, partition, offsets, time] = parts.as_slice() else {
                return None;
            };
            let (first, last) = offsets.strip_prefix("offsets=")?.split_once('-')?;
            let (start, end) = time
                .strip_prefix("time=")?
                .strip_suffix(".parquet")?
                .split_once('-')?;
            Some(BlockKey::new(
                tenant.strip_prefix("tenant=")?,
                partition.strip_prefix("partition=")?.parse().ok()?,
                first.parse().ok()?,
                last.parse().ok()?,
                TimeRange::new(start.parse().ok()?, end.parse().ok()?).ok()?,
            ))
        })
        .collect()
}

fn push_body() -> Value {
    json!({
        "streams": [{
            "stream": {"app": "api", "env": "prod"},
            "values": [
                ["10", "api error", {"trace_id": "abc"}],
                ["20", "api recovered"],
            ],
        }],
    })
}

/// The push the stop has to save: accepted with nothing waiting behind it.
fn late_push_body() -> Value {
    json!({
        "streams": [{
            "stream": {"app": "api", "env": "prod"},
            "values": [["30", "api stopping"]],
        }],
    })
}

/// Grants `tenant` every operation on the WAL topic.
///
/// The pinned in-process broker runs an authorizer and answers `DescribeAcls`
/// with the ACLs it holds, so the logs path reads its ACLs as configured. With
/// no ACL at all it would refuse every tenant, as Kafka's authorizer does. A
/// broker that answers `SECURITY_DISABLED` instead allows every tenant.
async fn grant_tenant_wal_access_for_test(bootstrap: &str, wal_topic: &str, tenant: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    let outcomes = admin
        .create_acls(&[AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: wal_topic.to_string(),
            pattern_type: PatternType::Literal,
            principal: format!("User:{tenant}"),
            host: "*".to_string(),
            operation: AclOperation::All,
            permission_type: PermissionType::Allow,
        }])
        .await
        .expect("create the tenant's WAL topic ACL");
    assert!(outcomes.iter().all(|outcome| outcome.error.is_none()));
}

async fn create_wal_topic(bootstrap: &str, wal_topic: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: wal_topic.to_string(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            secs(10),
        )
        .await
        .expect("create the logs WAL topic");
}

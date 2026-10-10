//! The Grafana container, the waits, and the in-process querier that the two
//! Docker-backed Grafana suites share.

use std::time::{Duration, Instant};

use assert2::assert;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{LabelIndex, LogBlockIndex as BlockIndex};
use krabka_observability::{InMemoryWalSink, QuerierState, distributor_router, loki_router};
use serde_json::Value;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::{net::TcpListener, sync::oneshot};
use tower::ServiceExt as _;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
pub const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// Grafana's default HTTP port.
pub const GRAFANA_PORT: u16 = 3000;

/// The tenant the provisioned datasources send on every request.
pub const TENANT: &str = "tenant-a";

/// One `name=value` pair of a query string.
#[derive(Clone, Debug)]
pub struct QueryPair {
    pub name: &'static str,
    pub value: String,
}

impl QueryPair {
    pub fn new(name: &'static str, value: impl std::fmt::Display) -> Self {
        Self {
            name,
            value: value.to_string(),
        }
    }
}

/// Encodes one query string from its pairs.
///
/// `reqwest` is built here without its `query` feature, which is what
/// `RequestBuilder::query` needs, so the pairs are encoded the way
/// `krabka-metrics-service`'s Grafana suite encodes its own.
pub fn query_string(pairs: &[QueryPair]) -> String {
    pairs
        .iter()
        .map(|pair| format!("{}={}", form_encode(pair.name), form_encode(&pair.value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn form_encode(text: &str) -> String {
    url::form_urlencoded::byte_serialize(text.as_bytes()).collect()
}

pub async fn start_grafana(datasources_yaml: &str) -> TestResult<ContainerAsync<GenericImage>> {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagreed
    // testcontainers pulled the image over the network and the suite ran
    // against whatever it got rather than against the pinned bytes.
    let tag = std::env::var("KRABKA_GRAFANA_IMAGE_TAG").expect(
        "KRABKA_GRAFANA_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under cargo, set it to \
         that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/grafana".to_string(), tag)
            .with_exposed_port(GRAFANA_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("HTTP Server Listen"))
            .with_env_var("GF_PLUGINS_PREINSTALL_DISABLED", "true")
            .with_copy_to(
                "/etc/grafana/provisioning/datasources/krabka.yaml",
                datasources_yaml.as_bytes().to_vec(),
            )
            // The backends run on the host: Krabka in this process, and Loki,
            // where a suite starts one, on its mapped port.
            .with_host("host.docker.internal", Host::HostGateway)
            .with_env_var("GF_AUTH_ANONYMOUS_ENABLED", "true")
            .with_env_var("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin")
            .with_env_var("GF_AUTH_BASIC_ENABLED", "false")
            .start(),
    )
    .await??)
}

/// An HTTP server at `base`, read through `client`.
#[derive(Clone, Copy)]
pub struct HttpBase<'a> {
    pub client: &'a reqwest::Client,
    /// The scheme, host and port, such as `http://127.0.0.1:3000`.
    pub base: &'a str,
}

impl HttpBase<'_> {
    /// Waits up to `timeout` for the server to answer `path` with a success
    /// status.
    pub async fn wait_for_ok(self, path: &str, timeout: Duration) -> TestResult {
        let base = self.base;
        if wait_for_success(self.client, &format!("{base}{path}"), timeout).await {
            Ok(())
        } else {
            Err(format!("{base}{path} did not become ready").into())
        }
    }

    /// Waits up to `timeout` for the Grafana at this base to provision
    /// datasource `uid`.
    pub async fn wait_for_datasource(self, uid: &str, timeout: Duration) -> TestResult {
        let base = self.base;
        if wait_for_success(
            self.client,
            &format!("{base}/api/datasources/uid/{uid}"),
            timeout,
        )
        .await
        {
            Ok(())
        } else {
            Err(format!("datasource {uid} was not provisioned on {base}").into())
        }
    }
}

async fn wait_for_success(client: &reqwest::Client, url: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if client
            .get(url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

/// A querier this process serves for a Grafana container to dial.
pub struct ServedQuerier {
    /// The host port the container dials through `host.docker.internal`.
    pub host_port: u16,
    shutdown: oneshot::Sender<()>,
}

impl ServedQuerier {
    pub fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

/// Seeds a querier through the real push door and serves it on the host.
///
/// The push goes through `distributor_router`, not straight into the sink, so
/// Krabka derives `detected_level` and `service_name` the way Loki's own
/// discovery does. The bind address is `0.0.0.0`, not `127.0.0.1`: the
/// container reaches this process over the host gateway, and a loopback-only
/// listener refuses that connection.
pub async fn serve_pushed(payload: &Value) -> TestResult<ServedQuerier> {
    let sink = InMemoryWalSink::default();
    let response = distributor_router(sink.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("content-type", "application/json")
                .header("X-Scope-OrgID", TENANT)
                .body(Body::from(payload.to_string()))?,
        )
        .await?;
    assert!(response.status() == StatusCode::NO_CONTENT);

    // `i64::MIN`: nothing has been compacted, so every record the distributor
    // wrote is in the querier's hot tail.
    let root = tempfile::tempdir()?.keep();
    let state = QuerierState::new(root, LabelIndex::default(), BlockIndex::default())
        .with_hot_tail(sink, i64::MIN);

    let listener = TcpListener::bind(("0.0.0.0", 0)).await?;
    let host_port = listener.local_addr()?.port();
    let (shutdown, stop) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, loki_router(state))
            .with_graceful_shutdown(async move {
                let _ = stop.await;
            })
            .await;
    });
    Ok(ServedQuerier {
        host_port,
        shutdown,
    })
}

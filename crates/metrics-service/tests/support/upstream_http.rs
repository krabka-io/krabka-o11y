//! The HTTP plumbing every suite that runs Krabka beside an upstream
//! container shares: the in-process WAL head Krabka serves from, the
//! container's mapped address, readiness, and `remote_write`.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use krabka_metrics::{
    WalRecord,
    distributor::{DistributorState, ProduceError, WalSink},
};
use krabka_promql::WalHead;
use reqwest::StatusCode;
use testcontainers::{ContainerAsync, GenericImage, core::IntoContainerPort};
use tokio::sync::oneshot;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// A WAL sink that applies each record straight to an in-memory head, so a
/// push is queryable without a broker.
struct WalHeadSink {
    head: WalHead,
}

#[async_trait::async_trait]
impl WalSink for WalHeadSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.head.apply_wal_record(&record);
        Ok(())
    }
}

pub async fn mapped_base_url(
    container: &ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port.tcp()).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

/// One `remote_write` push: where it goes, as which tenant, and its
/// snappy-compressed body.
pub struct RemoteWrite<'a> {
    pub base: &'a str,
    pub path: &'a str,
    pub tenant: Option<&'a str>,
    pub body: &'a [u8],
}

pub async fn post_remote_write(client: &reqwest::Client, push: RemoteWrite<'_>) -> TestResult {
    let RemoteWrite {
        base,
        path,
        tenant,
        body,
    } = push;
    let mut request = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body.to_vec());
    if let Some(tenant) = tenant {
        request = request.header("X-Scope-OrgID", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    if !(status == StatusCode::OK || status == StatusCode::NO_CONTENT) {
        let detail = response.text().await.unwrap_or_default();
        return Err(format!("remote_write to {base}{path} returned {status}: {detail}").into());
    }
    Ok(())
}

/// Polls `url` until it answers 2xx, for at most `within`.
pub async fn wait_for_http_ok(client: &reqwest::Client, url: &str, within: Duration) -> TestResult {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        if client
            .get(url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("{url} did not become ready").into())
}

/// Polls an instant-query response from `fetch_response` until its result is
/// non-empty, for at most `within`.
///
/// Returns `false` when the deadline passes first, so the caller can name the
/// query and server in its own error.
pub async fn wait_for_non_empty_result<F, Fut>(
    within: Duration,
    mut fetch_response: F,
) -> TestResult<bool>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = TestResult<serde_json::Value>>,
{
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        let response = fetch_response().await?;
        if response["data"]["result"]
            .as_array()
            .is_some_and(|series| !series.is_empty())
        {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(false)
}

/// An in-process Krabka serving remote-write and the query API over one WAL
/// head.
pub struct KrabkaServer {
    /// Loopback URL of the bound port, whatever address was bound.
    pub base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl KrabkaServer {
    /// Serves `query_router`, plus a distributor that writes into `head`, on
    /// `addr`.
    pub async fn start(
        query_router: axum::Router,
        head: WalHead,
        addr: SocketAddr,
    ) -> TestResult<Self> {
        let sink: Arc<dyn WalSink> = Arc::new(WalHeadSink { head });
        let distributor = Arc::new(DistributorState::new(sink));
        let router = query_router.merge(krabka_metrics::distributor::router(distributor));
        let (tx, rx) = oneshot::channel();
        let bound = krabka_metrics_service::serve_prometheus_router(
            addr,
            router,
            &krabka_observability::server_security::ServerSecurity::default(),
            async move {
                let _ = rx.await;
            },
        )
        .await?;
        Ok(Self {
            base_url: format!("http://127.0.0.1:{}", bound.port()),
            shutdown: Some(tx),
        })
    }

    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

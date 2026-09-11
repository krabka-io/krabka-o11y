//! The metrics write path, read path and ruler behind TLS and bearer tokens, on a real socket.
//!
//! Every test binds `127.0.0.1:0` with a certificate that `rcgen` makes for
//! the test, and loads a credentials file. It then talks to the listener with
//! `reqwest` for HTTP and with tonic for gRPC. The distributor and the
//! Prometheus API share one router, as the Grafana suite serves them, so a
//! query sees the sample that a push wrote.

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use bytes::Bytes;
use clap::Parser;
use krabka_blockstore::TenantId;
use krabka_metrics::{
    WalRecord,
    distributor::{DistributorState, ProduceError, WalSink, router as distributor_router},
    wire::pb,
};
use krabka_metrics_service::{
    BundledRulesError, install_bundled_rule_groups, prometheus_api_state_for_store,
    prometheus_router_for_store, serve_prometheus_router,
};
use krabka_observability::{
    RoleReadiness, readiness_router,
    server_security::{ServerSecurity, ServerSecurityArgs, install_crypto_provider},
};
use krabka_promql::{InMemoryMetricStore, WalHead, prometheus_router};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient,
    },
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
};
use prost::Message as _;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use serde_json::Value;
use tempfile::TempDir;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";
const GRAFANA_TOKEN: &str = "grafana-token-9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d";
const GRAFANA_TOKEN_SHA256: &str =
    "e87ca13d588249dfc0131a45fd6b399bb81889d7b9e238cdf17c969c7612adc7";
const RULER_TOKEN: &str = "ruler-token-1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e";
const RULER_TOKEN_SHA256: &str = "1e3f7a8ed3a2acefceed1373530134491af564ede086b56fda36059246ac5b3a";

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    security: ServerSecurityArgs,
}

/// A CA, a server certificate for `localhost` and `127.0.0.1`, a credentials
/// file, and the internal client token, all in one temporary directory.
struct Pki {
    dir: TempDir,
    ca_pem: String,
}

impl Pki {
    fn new() -> Self {
        let dir = TempDir::new().expect("a temporary directory");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(DnType::CommonName, "krabka metrics test ca");
        let authority = CertifiedIssuer::self_signed(params, KeyPair::generate().expect("a key"))
            .expect("a self-signed CA");

        let mut params =
            CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
                .expect("valid parameters");
        params.distinguished_name.push(DnType::CommonName, "server");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate().expect("a key");
        let certificate = params
            .signed_by(&key, &authority)
            .expect("a signed server certificate");

        let pki = Self {
            dir,
            ca_pem: authority.pem(),
        };
        pki.write("ca.pem", &pki.ca_pem);
        pki.write("server.pem", certificate.pem());
        pki.write("server-key.pem", key.serialize_pem());
        pki.write(
            "credentials.yaml",
            format!(
                "principals:\n  - name: grafana\n    token_sha256: [\"{GRAFANA_TOKEN_SHA256}\"]\n    tenants: [\"{TENANT_A}\"]\n  - name: ruler\n    token_sha256: [\"{RULER_TOKEN_SHA256}\"]\n    tenants: [\"{TENANT_A}\"]\n"
            ),
        );
        pki.write("ruler-token", format!("{RULER_TOKEN}\n"));
        pki
    }

    fn write(&self, name: &str, contents: impl AsRef<[u8]>) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, contents).expect("the temporary file is writable");
        path
    }

    fn path(&self, name: &str) -> String {
        self.dir.path().join(name).display().to_string()
    }

    /// TLS and authentication, with `extra` flags after them.
    fn security(&self, extra: &[&str]) -> ServerSecurity {
        let (server, key, credentials) = (
            self.path("server.pem"),
            self.path("server-key.pem"),
            self.path("credentials.yaml"),
        );
        let argv = [
            "test",
            "--server-tls-cert-path",
            &server,
            "--server-tls-key-path",
            &key,
            "--auth-credentials-config",
            &credentials,
        ]
        .into_iter()
        .chain(extra.iter().copied());
        Cli::try_parse_from(argv)
            .expect("the flags parse")
            .security
            .load()
            .expect("the flags load")
    }

    fn https_client(&self) -> reqwest::Client {
        let ca = reqwest::Certificate::from_pem(self.ca_pem.as_bytes()).expect("a PEM CA");
        reqwest::Client::builder()
            .tls_certs_only([ca])
            .build()
            .expect("the client builds")
    }
}

/// The WAL sink of the served distributor: it applies each record to the head
/// that the Prometheus API reads, and keeps a copy for the test.
struct RecordingHeadSink {
    head: WalHead,
    records: Mutex<Vec<WalRecord>>,
}

#[async_trait::async_trait]
impl WalSink for RecordingHeadSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.head.apply_wal_record(&record);
        self.records
            .lock()
            .expect("no test panics while holding the lock")
            .push(record);
        Ok(())
    }
}

impl RecordingHeadSink {
    fn records(&self) -> Vec<WalRecord> {
        self.records
            .lock()
            .expect("no test panics while holding the lock")
            .clone()
    }
}

/// The distributor, the Prometheus API and `/ready`, served with `security`.
async fn serve_stack(security: &ServerSecurity) -> (SocketAddr, Arc<RecordingHeadSink>) {
    let head = WalHead::new();
    let sink = Arc::new(RecordingHeadSink {
        head: head.clone(),
        records: Mutex::new(Vec::new()),
    });
    let router = prometheus_router_for_store(head)
        .merge(distributor_router(Arc::new(DistributorState::new(
            Arc::clone(&sink) as Arc<dyn WalSink>,
        ))))
        .merge(readiness_router(RoleReadiness::new()));
    // The test runtime stops the server when the test ends.
    let addr = serve_prometheus_router(
        "127.0.0.1:0".parse().expect("a socket address"),
        router,
        security,
        std::future::pending(),
    )
    .await
    .expect("the stack serves");
    (addr, sink)
}

fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the epoch");
    i64::try_from(elapsed.as_millis()).expect("the time fits in i64")
}

fn remote_write_body(timestamp_ms: i64) -> Vec<u8> {
    let request = pb::v1::WriteRequest {
        timeseries: vec![pb::v1::TimeSeries {
            labels: vec![
                pb::v1::Label {
                    name: "__name__".to_owned(),
                    value: "up".to_owned(),
                },
                pb::v1::Label {
                    name: "job".to_owned(),
                    value: "api".to_owned(),
                },
            ],
            samples: vec![pb::v1::Sample {
                value: 1.0,
                timestamp: timestamp_ms,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy compresses")
}

fn push(
    client: &reqwest::Client,
    addr: SocketAddr,
    tenant: &str,
    timestamp_ms: i64,
) -> reqwest::RequestBuilder {
    client
        .post(format!("https://{addr}/api/v1/push"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Scope-OrgID", tenant)
        .body(remote_write_body(timestamp_ms))
}

/// An instant query for `up` at the first whole second after `sample_ms`, so the sample is in its lookback window.
fn query(
    client: &reqwest::Client,
    addr: SocketAddr,
    tenant: &str,
    sample_ms: i64,
) -> reqwest::RequestBuilder {
    client
        .get(format!(
            "https://{addr}/prometheus/api/v1/query?query=up&time={}",
            sample_ms / 1000 + 1
        ))
        .header("X-Scope-OrgID", tenant)
}

#[tokio::test]
async fn a_granted_principal_pushes_and_queries_its_tenant() {
    install_crypto_provider();
    let pki = Pki::new();
    let (addr, sink) = serve_stack(&pki.security(&[])).await;
    let client = pki.https_client();
    let timestamp_ms = now_ms();

    let pushed = push(&client, addr, TENANT_A, timestamp_ms)
        .bearer_auth(GRAFANA_TOKEN)
        .send()
        .await
        .expect("the push is sent");
    let answer = query(&client, addr, TENANT_A, timestamp_ms)
        .bearer_auth(GRAFANA_TOKEN)
        .send()
        .await
        .expect("the query is sent");
    let status = answer.status();
    let body: Value = answer.json().await.expect("a JSON answer");

    check!(pushed.status() == reqwest::StatusCode::NO_CONTENT);
    check!(sink.records().len() == 1);
    check!(status == reqwest::StatusCode::OK);
    check!(body["data"]["result"][0]["metric"]["job"] == "api");
    check!(body["data"]["result"][0]["value"][1] == "1");
}

#[tokio::test]
async fn a_principal_without_the_tenant_is_refused_on_push_and_query() {
    install_crypto_provider();
    let pki = Pki::new();
    let (addr, sink) = serve_stack(&pki.security(&[])).await;
    let client = pki.https_client();
    let timestamp_ms = now_ms();

    let pushed = push(&client, addr, TENANT_B, timestamp_ms)
        .bearer_auth(GRAFANA_TOKEN)
        .send()
        .await
        .expect("the push is sent");
    let pushed_status = pushed.status();
    let pushed_body = pushed.text().await.expect("a text answer");
    let queried = query(&client, addr, TENANT_B, timestamp_ms)
        .bearer_auth(GRAFANA_TOKEN)
        .send()
        .await
        .expect("the query is sent");
    let queried_status = queried.status();
    let queried_body = queried.text().await.expect("a text answer");

    let refusal = format!("principal \"grafana\" is not allowed to access tenant \"{TENANT_B}\"\n");
    check!(pushed_status == reqwest::StatusCode::FORBIDDEN);
    check!(pushed_body == refusal);
    check!(queried_status == reqwest::StatusCode::FORBIDDEN);
    check!(queried_body == refusal);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn a_request_without_a_valid_credential_is_unauthorized() {
    install_crypto_provider();
    let pki = Pki::new();
    let (addr, sink) = serve_stack(&pki.security(&[])).await;
    let client = pki.https_client();
    let timestamp_ms = now_ms();

    let statuses = [
        push(&client, addr, TENANT_A, timestamp_ms).send().await,
        push(&client, addr, TENANT_A, timestamp_ms)
            .bearer_auth("not-a-configured-token")
            .send()
            .await,
        query(&client, addr, TENANT_A, timestamp_ms).send().await,
        query(&client, addr, TENANT_A, timestamp_ms)
            .bearer_auth("not-a-configured-token")
            .send()
            .await,
    ]
    .map(|answer| answer.expect("the request is sent").status());

    check!(statuses == [reqwest::StatusCode::UNAUTHORIZED; 4]);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn ready_answers_without_a_credential() {
    install_crypto_provider();
    let pki = Pki::new();
    let (addr, _sink) = serve_stack(&pki.security(&[])).await;

    let answer = pki
        .https_client()
        .get(format!("https://{addr}/ready"))
        .send()
        .await
        .expect("the probe is sent");
    let status = answer.status();
    let body = answer.text().await.expect("a text answer");

    check!(status == reqwest::StatusCode::OK);
    check!(body == "ready\n");
}

fn otlp_export(tenant: &str) -> tonic::Request<ExportMetricsServiceRequest> {
    let time_unix_nano = u64::try_from(now_ms()).expect("the time is after the epoch") * 1_000_000;
    let mut request = tonic::Request::new(ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "system_cpu_utilization".to_owned(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano,
                            value: Some(number_data_point::Value::AsDouble(0.5)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {GRAFANA_TOKEN}")
            .parse()
            .expect("a metadata value"),
    );
    request
        .metadata_mut()
        .insert("x-scope-orgid", tenant.parse().expect("a metadata value"));
    request
}

#[tokio::test]
async fn an_otlp_grpc_export_from_a_principal_without_the_tenant_is_permission_denied() {
    install_crypto_provider();
    let pki = Pki::new();
    let (addr, sink) = serve_stack(&pki.security(&[])).await;
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&pki.ca_pem))
        .domain_name("localhost");
    let channel = tonic::transport::Endpoint::from_shared(format!("https://{addr}"))
        .expect("a valid endpoint")
        .tls_config(tls)
        .expect("a valid TLS config")
        .connect()
        .await
        .expect("the channel connects");
    let mut client = MetricsServiceClient::new(channel);

    let refused = client
        .export(otlp_export(TENANT_B))
        .await
        .expect_err("a principal without the tenant is refused");
    let records_after_refusal = sink.records().len();
    let granted = client.export(otlp_export(TENANT_A)).await;

    check!(refused.code() == tonic::Code::PermissionDenied);
    check!(records_after_refusal == 0);
    check!(granted.is_ok(), "{granted:?}");
    check!(!sink.records().is_empty());
}

const RULE_FILE: &str = "groups:\n  - name: api-up\n    rules:\n      - record: job:up:sum\n        expr: sum by (job) (up)\n";

/// With authentication on, the ruler posts its bundled groups to its own API
/// with the internal client credential, over TLS. Without that credential,
/// the API answers 401 and the start stops.
#[tokio::test]
async fn the_ruler_installs_its_bundled_rules_with_the_internal_client_credential() {
    install_crypto_provider();
    let pki = Pki::new();
    let (token, ca) = (pki.path("ruler-token"), pki.path("ca.pem"));
    let security = pki.security(&[
        "--internal-client-token-path",
        &token,
        "--internal-client-tls-ca-path",
        &ca,
    ]);
    let state = prometheus_api_state_for_store(InMemoryMetricStore::new());
    let listener = serve_prometheus_router(
        "127.0.0.1:0".parse().expect("a socket address"),
        prometheus_router(Arc::clone(&state)),
        &security,
        std::future::pending(),
    )
    .await
    .expect("the ruler API serves");
    let rule_file = pki.write("api-rules.yaml", RULE_FILE);
    let tenant_a = TenantId::new(TENANT_A).expect("a valid tenant id");
    let tenant_b = TenantId::new(TENANT_B).expect("a valid tenant id");

    let installed = install_bundled_rule_groups(listener, &security, &rule_file, &tenant_a).await;
    let without_token = install_bundled_rule_groups(
        listener,
        &pki.security(&["--internal-client-tls-ca-path", &ca]),
        &rule_file,
        &tenant_a,
    )
    .await;
    let other_tenant =
        install_bundled_rule_groups(listener, &security, &rule_file, &tenant_b).await;

    check!(installed.ok() == Some(vec!["api-up".to_owned()]));
    check!(
        state
            .ruler_rule_set(&tenant_a)
            .into_keys()
            .collect::<Vec<_>>()
            == vec!["api-rules".to_owned()]
    );
    check!(
        matches!(
            without_token,
            Err(BundledRulesError::Rejected { status, .. })
                if status == reqwest::StatusCode::UNAUTHORIZED
        ),
        "{without_token:?}"
    );
    check!(
        matches!(
            other_tenant,
            Err(BundledRulesError::Rejected { status, .. })
                if status == reqwest::StatusCode::FORBIDDEN
        ),
        "{other_tenant:?}"
    );
    check!(state.ruler_rule_set(&tenant_b).is_empty());
}

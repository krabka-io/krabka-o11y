//! The runtime overrides file, from the bytes on disk to the verdict a push
//! gets back.
//!
//! `krabka-metrics` reads a Mimir-style `runtime.yaml` and gives each tenant
//! the limits it names. This test writes such a file, loads it the way the
//! binary's `--runtime-overrides` flag does, boots the distributor on a real
//! ephemeral `127.0.0.1:0` socket, and pushes `remote_write` under two tenants.
//! The listed tenant gets the limits in the file. An unlisted one gets the
//! defaults.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use assert2::check;
use async_trait::async_trait;
use bytes::Bytes;
use krabka_metrics::{
    OverridesProvider, WalRecord,
    distributor::{DistributorState, ProduceError, WalSink, serve},
    wire::pb,
};
use krabka_observability::server_security::ServerSecurity;
use prost::Message;

const TIGHT: &str = "tenant-tight";
const LOOSE: &str = "tenant-loose";

const RUNTIME_YAML: &str = r"
overrides:
  tenant-tight:
    max_series_per_request: 1
    max_samples_per_series: 1
";

/// In-memory WAL sink. It records every appended `WalRecord` and never touches
/// a broker.
#[derive(Default)]
struct RecordingSink {
    records: Mutex<Vec<WalRecord>>,
}

#[async_trait]
impl WalSink for RecordingSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.records
            .lock()
            .expect("recording sink poisoned")
            .push(record);
        Ok(())
    }
}

impl RecordingSink {
    fn len(&self) -> usize {
        self.records.lock().expect("recording sink poisoned").len()
    }
}

/// A snappy-compressed `remote_write` v1 body holding `series` series with
/// `samples` samples each.
fn remote_write_v1_body(series: usize, samples: usize) -> Vec<u8> {
    let request = pb::v1::WriteRequest {
        timeseries: (0..series)
            .map(|index| pb::v1::TimeSeries {
                labels: vec![pb::v1::Label {
                    name: "__name__".into(),
                    value: format!("series_{index}"),
                }],
                samples: (0..samples)
                    .map(|sample| pb::v1::Sample {
                        value: 1.0,
                        timestamp: 100 + i64::try_from(sample).expect("small"),
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy compress")
}

/// Reads the overrides file the way the binary's loader does.
fn load_runtime_overrides(path: &std::path::Path) -> OverridesProvider {
    let yaml = std::fs::read_to_string(path).expect("read runtime overrides");
    OverridesProvider::from_yaml(&yaml).expect("parse runtime overrides")
}

async fn boot_distributor(overrides: OverridesProvider) -> (SocketAddr, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    let state = Arc::new(DistributorState::new(sink.clone()).with_overrides(overrides));
    let addr = serve(
        "127.0.0.1:0".parse().expect("socket addr"),
        state,
        &ServerSecurity::default(),
        std::future::pending(),
    )
    .await
    .expect("serve distributor");
    (addr, sink)
}

async fn push(
    client: &reqwest::Client,
    addr: SocketAddr,
    tenant: &str,
    body: Vec<u8>,
) -> reqwest::StatusCode {
    client
        .post(format!("http://{addr}/api/v1/push"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Scope-OrgID", tenant)
        .body(body)
        .send()
        .await
        .expect("send push")
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_runtime_overrides_file_sets_the_limits_a_push_is_judged_by() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("runtime.yaml");
    std::fs::write(&path, RUNTIME_YAML).expect("write runtime overrides");

    let (addr, sink) = boot_distributor(load_runtime_overrides(&path)).await;
    let client = reqwest::Client::new();

    // The file caps the listed tenant at one series and one sample per series.
    check!(
        push(&client, addr, TIGHT, remote_write_v1_body(2, 1)).await
            == reqwest::StatusCode::BAD_REQUEST,
        "two series exceed the file's cap of one"
    );
    check!(
        push(&client, addr, TIGHT, remote_write_v1_body(1, 2)).await
            == reqwest::StatusCode::BAD_REQUEST,
        "two samples exceed the file's cap of one"
    );
    check!(
        push(&client, addr, TIGHT, remote_write_v1_body(1, 1)).await
            == reqwest::StatusCode::NO_CONTENT,
        "one series of one sample is exactly the cap"
    );

    // A tenant the file does not list keeps the built-in defaults, so the
    // same load it refused for the listed tenant goes through.
    check!(
        push(&client, addr, LOOSE, remote_write_v1_body(2, 1)).await
            == reqwest::StatusCode::NO_CONTENT
    );

    check!(sink.len() == 3, "only the accepted pushes appended");
}

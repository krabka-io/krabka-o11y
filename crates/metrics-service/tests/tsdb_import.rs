#![recursion_limit = "512"]

//! Prometheus TSDB block import through the Mimir block-upload route.
//!
//! Each test uploads the checked-in Prometheus block the way `mimirtool
//! backfill` does: `start` with the block's `meta.json`, one `files` request
//! per file, `finish`, then `check`. The in-process tests compare what the
//! query API answers with the samples that the Prometheus TSDB library reads
//! from the same block. The differential test serves the same block from a
//! real Prometheus and compares the two query APIs.
//!
//! Cargo ignores the differential by default, because it starts
//! `mirror.gcr.io/prom/prometheus`. Run it with:
//!
//! `bazel test --config=docker //crates/metrics-service:tsdb_import_docker_test`

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use assert2::{assert, check};
use futures::TryStreamExt as _;
use krabka_metrics_service::{
    DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK, MimirTenantAdminState, RefreshingMetricBlockStore,
    mimir_tenant_admin_router, serve_prometheus_router,
};
use krabka_observability::server_security::ServerSecurity;
use krabka_promql::{EngineOpts, PrometheusApiState, WalHead, prometheus_router};
use krabka_units::prelude::*;
use object_store::{GetOptions, GetResult, ObjectStore, memory::InMemory, path::Path};
use reqwest::StatusCode;
use serde_json::{Value, json};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::oneshot;
use tsdb_fixture::{
    FIXTURE_MAX_TIME, FIXTURE_MIN_TIME, FIXTURE_ULID, FixtureFiles, expected_samples, fixture_files,
};

use self::crashing_store::{CrashPoint, CrashingStore};

// This suite uses only `normalize`, which the other differential suites share.
#[allow(dead_code)]
#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

#[path = "../../metrics/tests/support/crashing_store.rs"]
mod crashing_store;
#[path = "support/pinned_prometheus_image.rs"]
mod pinned_prometheus_image;
#[path = "../../metrics/tests/support/tsdb_fixture.rs"]
mod tsdb_fixture;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const TENANT: &str = "tenant-a";
const OTHER_ULID: &str = "01M3MJXM7R4M5X4Q4CKHW5Q8N1";
const MANIFEST_PREFIX: &str = "metrics";
/// The bits of the Prometheus stale marker.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
/// The deadline for a container to start. `AsyncRunner::start` has no bound
/// of its own.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);
const PROMETHEUS_PORT: u16 = 9090;
/// A lookback for requests without a time range that reaches the 2025
/// fixture block. Cardinality routes and `/api/v1/status/tsdb` take no time
/// range, so Krabka reads only the blocks inside this lookback.
const FIXTURE_LOOKBACK: Time = days(100 * 365);
/// The features of the `diff_prometheus` oracle. With them on, Prometheus
/// annotates `rate` as Krabka does.
const PROMETHEUS_FEATURES: &str = "native-histograms,promql-experimental-functions,\
     promql-duration-expr,promql-extended-range-selectors,type-and-unit-labels";

/// A Krabka query and block-upload API on a real socket, over one in-memory
/// object store.
struct Krabka {
    base: String,
    store: Arc<dyn ObjectStore>,
    client: reqwest::Client,
    _shutdown: oneshot::Sender<()>,
}

#[derive(Debug, PartialEq, Eq)]
struct UploadResponse {
    status: StatusCode,
    body: String,
}

struct DeclaredUploadFile {
    path: String,
    size_bytes: usize,
}

struct UploadedFile {
    path: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct QueryParameter {
    name: String,
    value: String,
}

impl QueryParameter {
    fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// Two Krabkas holding the fixture block: one with the default unbounded
/// lookback, which does not reach the fixture's samples, and one whose
/// lookback does.
struct LookbackPair {
    recent: Krabka,
    reaching: Krabka,
}

impl LookbackPair {
    async fn with_fixture_uploaded() -> TestResult<Self> {
        let recent = Krabka::start().await?;
        recent.upload(FIXTURE_ULID, &UploadBlock::fixture()).await?;
        let reaching =
            Krabka::start_with_lookback(Arc::new(InMemory::new()), FIXTURE_LOOKBACK).await?;
        reaching
            .upload(FIXTURE_ULID, &UploadBlock::fixture())
            .await?;
        Ok(Self { recent, reaching })
    }
}

impl Krabka {
    async fn start() -> TestResult<Self> {
        Self::start_with(Arc::new(InMemory::new())).await
    }

    async fn start_with(store: Arc<dyn ObjectStore>) -> TestResult<Self> {
        Self::start_with_lookback(store, DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK).await
    }

    /// Starts Krabka with `lookback` as the window of a request that has no
    /// time range, as `--unbounded-compatibility-lookback` sets it.
    async fn start_with_lookback(store: Arc<dyn ObjectStore>, lookback: Time) -> TestResult<Self> {
        let head = WalHead::new();
        let query = Arc::new(
            RefreshingMetricBlockStore::new(
                Arc::clone(&store),
                "memory:///".parse()?,
                MANIFEST_PREFIX,
                head.clone(),
            )
            .with_unbounded_compatibility_lookback(lookback),
        );
        let api = PrometheusApiState::new(Arc::clone(&query), EngineOpts::default());
        let router = prometheus_router(Arc::new(api)).merge(mimir_tenant_admin_router(
            MimirTenantAdminState::new(Arc::clone(&store), query, head),
        ));
        let channel = oneshot::channel();
        let shutdown = channel.0;
        let stopped = channel.1;
        let addr: SocketAddr = "127.0.0.1:0".parse()?;
        let bound = serve_prometheus_router(addr, router, &ServerSecurity::default(), async {
            let _ = stopped.await;
        })
        .await?;
        Ok(Self {
            base: format!("http://{bound}"),
            store,
            client: reqwest::Client::new(),
            _shutdown: shutdown,
        })
    }

    fn upload_url(&self, ulid: &str, step: &str) -> String {
        format!("{}/api/v1/upload/block/{ulid}/{step}", self.base)
    }

    async fn post(&self, url: String, body: Vec<u8>) -> TestResult<UploadResponse> {
        let response = self
            .client
            .post(url)
            .header("X-Scope-OrgID", TENANT)
            .body(body)
            .send()
            .await?;
        Ok(UploadResponse {
            status: response.status(),
            body: response.text().await?,
        })
    }

    /// Uploads `block` under `ulid`, and returns the `check` answer.
    async fn upload(&self, ulid: &str, block: &UploadBlock) -> TestResult<Value> {
        let response = self
            .post(self.upload_url(ulid, "start"), block.meta(ulid))
            .await?;
        assert!(
            response.status == StatusCode::OK,
            "start: {}",
            response.body
        );
        for file in &block.uploaded {
            let url = format!(
                "{}?path={}",
                self.upload_url(ulid, "files"),
                encode(&file.path)
            );
            let response = self.post(url, file.bytes.clone()).await?;
            assert!(
                response.status == StatusCode::OK,
                "file {}: {}",
                file.path,
                response.body
            );
        }
        let response = self
            .post(self.upload_url(ulid, "finish"), Vec::new())
            .await?;
        assert!(
            response.status == StatusCode::OK,
            "finish: {}",
            response.body
        );
        self.get_json(&self.upload_url(ulid, "check"), &[]).await
    }

    async fn get_json(&self, url: &str, params: &[QueryParameter]) -> TestResult<Value> {
        let mut url = url::Url::parse(url)?;
        let mut query = url.query_pairs_mut();
        for parameter in params {
            query.append_pair(&parameter.name, &parameter.value);
        }
        drop(query);
        let text = self
            .client
            .get(url)
            .header("X-Scope-OrgID", TENANT)
            .send()
            .await?
            .text()
            .await?;
        Ok(serde_json::from_str(&text)?)
    }

    async fn keys_under(&self, prefix: &str) -> TestResult<BTreeSet<String>> {
        Ok(self
            .store
            .list(Some(&Path::from(prefix)))
            .map_ok(|object| object.location.to_string())
            .try_collect()
            .await?)
    }

    async fn manifest_keys(&self) -> TestResult<BTreeSet<String>> {
        Ok(self
            .keys_under(MANIFEST_PREFIX)
            .await?
            .into_iter()
            .filter(|key| {
                std::path::Path::new(key)
                    .extension()
                    .is_some_and(|extension| extension == "index")
            })
            .collect())
    }
}

/// The files of one upload, as `meta.json` declares them and as the client
/// sends them.
struct UploadBlock {
    min_time: i64,
    max_time: i64,
    labels: BTreeMap<String, String>,
    declared: Vec<DeclaredUploadFile>,
    uploaded: Vec<UploadedFile>,
}

struct ExternalLabelCase {
    name: &'static str,
    value: &'static str,
    status: StatusCode,
    body: &'static str,
}

struct CrashPointCase {
    key_part: &'static str,
    occurrence: usize,
    live: bool,
}

struct RangeWindow {
    from: i64,
    to: i64,
    step: &'static str,
}

struct SeriesWindow {
    from: i64,
    to: i64,
}

impl UploadBlock {
    fn new(files: FixtureFiles) -> Self {
        let uploaded = vec![
            UploadedFile {
                path: "index".to_owned(),
                bytes: files.index,
            },
            UploadedFile {
                path: "chunks/000001".to_owned(),
                bytes: files.chunks,
            },
            UploadedFile {
                path: "tombstones".to_owned(),
                bytes: files.tombstones,
            },
        ];
        Self {
            min_time: FIXTURE_MIN_TIME,
            max_time: FIXTURE_MAX_TIME,
            labels: {
                let mut labels = BTreeMap::new();
                labels.insert("__org_id__".to_owned(), TENANT.to_owned());
                labels
            },
            declared: uploaded
                .iter()
                .map(|file| DeclaredUploadFile {
                    path: file.path.clone(),
                    size_bytes: file.bytes.len(),
                })
                .collect(),
            uploaded,
        }
    }

    fn fixture() -> Self {
        Self::new(fixture_files())
    }

    fn meta(&self, ulid: &str) -> Vec<u8> {
        let files: Vec<Value> = std::iter::once(json!({"rel_path": "meta.json"}))
            .chain(
                self.declared
                    .iter()
                    .map(|file| json!({"rel_path": file.path, "size_bytes": file.size_bytes})),
            )
            .collect();
        serde_json::to_vec(&json!({
            "ulid": ulid,
            "minTime": self.min_time,
            "maxTime": self.max_time,
            "version": 1,
            "thanos": {"labels": self.labels, "files": files},
        }))
        .expect("encode meta.json")
    }
}

fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn seconds(ms: i64) -> String {
    format!("{}.{:03}", ms.div_euclid(1000), ms.rem_euclid(1000))
}

/// One sample in the query API's float result.
#[derive(Debug, PartialEq, Eq)]
struct FloatBitsSample {
    timestamp_ms: i64,
    value_bits: u64,
}

/// One histogram sample in the query API's matrix result.
#[derive(Debug, PartialEq, Eq)]
struct HistogramBitsSample {
    timestamp_ms: i64,
    count_bits: u64,
    sum_bits: u64,
    bucket_count_bits: Vec<u64>,
}

/// One series as the query API returns it from a range selector.
#[derive(Debug, PartialEq, Eq)]
struct Series {
    labels: BTreeMap<String, String>,
    floats: Vec<FloatBitsSample>,
    histograms: Vec<HistogramBitsSample>,
}

fn sorted_bits(mut values: Vec<f64>) -> Vec<u64> {
    values.retain(|value| *value != 0.0);
    values.sort_by(f64::total_cmp);
    values.into_iter().map(f64::to_bits).collect()
}

fn hex_f64(value: &Value) -> f64 {
    f64::from_bits(
        u64::from_str_radix(value.as_str().expect("a hex string"), 16).expect("hex bits"),
    )
}

fn hex_list(value: &Value) -> Vec<f64> {
    value
        .as_array()
        .expect("a list")
        .iter()
        .map(hex_f64)
        .collect()
}

/// The samples that Prometheus reads from the fixture, as a range selector
/// returns them: without stale markers.
fn expected_series() -> Vec<Series> {
    expected_samples()
        .as_array()
        .expect("the expected samples are an array")
        .iter()
        .map(|series| {
            let labels = serde_json::from_value(series["labels"].clone()).expect("labels");
            let floats = series["floats"]
                .as_array()
                .expect("floats")
                .iter()
                .map(|sample| {
                    let t = sample["t"].as_i64().expect("t");
                    FloatBitsSample {
                        timestamp_ms: t,
                        value_bits: hex_f64(&sample["v"]).to_bits(),
                    }
                })
                .filter(|sample| sample.value_bits != STALE_NAN_BITS)
                .collect();
            let histograms = series["histograms"]
                .as_array()
                .expect("histograms")
                .iter()
                .filter(|histogram| hex_f64(&histogram["sum"]).to_bits() != STALE_NAN_BITS)
                .map(|histogram| {
                    let mut buckets = hex_list(&histogram["positive_counts"]);
                    buckets.extend(hex_list(&histogram["negative_counts"]));
                    buckets.push(hex_f64(&histogram["zero_count"]));
                    HistogramBitsSample {
                        timestamp_ms: histogram["t"].as_i64().expect("t"),
                        count_bits: hex_f64(&histogram["count"]).to_bits(),
                        sum_bits: hex_f64(&histogram["sum"]).to_bits(),
                        bucket_count_bits: sorted_bits(buckets),
                    }
                })
                .collect();
            Series {
                labels,
                floats,
                histograms,
            }
        })
        .collect()
}

fn parse_ms(value: &Value) -> i64 {
    let seconds = value.as_f64().expect("a timestamp");
    #[expect(
        clippy::cast_possible_truncation,
        reason = "fixture timestamps are exact milliseconds"
    )]
    let ms = (seconds * 1000.0).round() as i64;
    ms
}

fn parse_f64(value: &Value) -> f64 {
    value
        .as_str()
        .expect("a sample string")
        .parse()
        .expect("a sample value")
}

/// Reads a matrix result into [`Series`], sorted by labels.
fn matrix_series(response: &Value) -> Vec<Series> {
    let mut series: Vec<Series> = response["data"]["result"]
        .as_array()
        .expect("a matrix result")
        .iter()
        .map(|entry| {
            let labels = serde_json::from_value(entry["metric"].clone()).expect("labels");
            let floats = entry["values"].as_array().map_or_default(|values| {
                values
                    .iter()
                    .map(|point| FloatBitsSample {
                        timestamp_ms: parse_ms(&point[0]),
                        value_bits: parse_f64(&point[1]).to_bits(),
                    })
                    .collect()
            });
            let histograms = entry["histograms"].as_array().map_or_default(|values| {
                values
                    .iter()
                    .map(|point| {
                        let histogram = &point[1];
                        let buckets = histogram["buckets"].as_array().map_or_default(|buckets| {
                            buckets.iter().map(|bucket| parse_f64(&bucket[3])).collect()
                        });
                        HistogramBitsSample {
                            timestamp_ms: parse_ms(&point[0]),
                            count_bits: parse_f64(&histogram["count"]).to_bits(),
                            sum_bits: parse_f64(&histogram["sum"]).to_bits(),
                            bucket_count_bits: sorted_bits(buckets),
                        }
                    })
                    .collect()
            });
            Series {
                labels,
                floats,
                histograms,
            }
        })
        .collect();
    series.sort_by(|left, right| left.labels.cmp(&right.labels));
    series
}

async fn all_imported_samples(krabka: &Krabka) -> TestResult<Vec<Series>> {
    let response = krabka
        .get_json(
            &format!("{}/api/v1/query", krabka.base),
            &[
                QueryParameter::new("query", r#"{__name__=~"imported_.+"}[3h]"#),
                QueryParameter::new("time", seconds(FIXTURE_MAX_TIME)),
            ],
        )
        .await?;
    check!(response["status"] == "success", "{response}");
    Ok(matrix_series(&response))
}

#[tokio::test]
async fn an_uploaded_prometheus_block_returns_the_samples_that_prometheus_reads() -> TestResult {
    let krabka = Krabka::start().await?;

    let check_body = krabka.upload(FIXTURE_ULID, &UploadBlock::fixture()).await?;

    check!(check_body == json!({"result": "complete"}));
    check!(all_imported_samples(&krabka).await? == expected_series());
    let range = [
        QueryParameter::new("start", seconds(FIXTURE_MIN_TIME)),
        QueryParameter::new("end", seconds(FIXTURE_MAX_TIME)),
    ];
    let mut series_params = range.to_vec();
    series_params.push(QueryParameter::new(
        "match[]",
        r#"{__name__=~"imported_.+"}"#,
    ));
    let series = krabka
        .get_json(&format!("{}/api/v1/series", krabka.base), &series_params)
        .await?;
    let expected_labels: Vec<Value> = expected_series()
        .into_iter()
        .map(|series| json!(series.labels))
        .collect();
    check!(series == json!({"status": "success", "data": expected_labels}));
    let labels = krabka
        .get_json(&format!("{}/api/v1/labels", krabka.base), &range)
        .await?;
    check!(labels == json!({"status": "success", "data": ["__name__", "instance", "job"]}));
    let instances = krabka
        .get_json(
            &format!("{}/api/v1/label/instance/values", krabka.base),
            &range,
        )
        .await?;
    check!(instances == json!({"status": "success", "data": ["i1", "zürich"]}));
    Ok(())
}

#[tokio::test]
async fn a_reuploaded_prometheus_block_adds_no_samples() -> TestResult {
    let krabka = Krabka::start().await?;
    krabka.upload(FIXTURE_ULID, &UploadBlock::fixture()).await?;
    let manifests = krabka.manifest_keys().await?;
    let samples = all_imported_samples(&krabka).await?;

    let same_ulid = krabka
        .post(
            krabka.upload_url(FIXTURE_ULID, "start"),
            UploadBlock::fixture().meta(FIXTURE_ULID),
        )
        .await?;
    let other_ulid = krabka.upload(OTHER_ULID, &UploadBlock::fixture()).await?;

    check!(same_ulid.status == StatusCode::CONFLICT);
    check!(same_ulid.body == "block already exists\n");
    check!(other_ulid == json!({"result": "complete", "existingBlock": FIXTURE_ULID}));
    check!(krabka.manifest_keys().await? == manifests);
    check!(all_imported_samples(&krabka).await? == samples);
    Ok(())
}

/// A case name, the change to the fixture upload, and the `check` error.
struct CorruptCase {
    name: &'static str,
    corrupt: fn(&mut UploadBlock),
    error: &'static str,
}

/// Flips the low bit of the byte at `position` of the uploaded file `path`.
fn flip(block: &mut UploadBlock, path: &str, position: usize) {
    let file = block
        .uploaded
        .iter_mut()
        .find(|file| file.path == path)
        .expect("the file is uploaded");
    let position = position.min(file.bytes.len() - 1);
    file.bytes[position] ^= 0x01;
}

#[tokio::test]
async fn an_invalid_prometheus_block_fails_and_leaves_no_index_entry() -> TestResult {
    let cases: [CorruptCase; 6] = [
        CorruptCase {
            name: "flipped index TOC checksum",
            corrupt: |block| flip(block, "index", usize::MAX),
            error: "invalid Prometheus TSDB block: checksum mismatch in index TOC",
        },
        CorruptCase {
            name: "flipped chunk byte",
            corrupt: |block| flip(block, "chunks/000001", 12),
            error: "invalid Prometheus TSDB block: checksum mismatch in chunk at reference 0x8",
        },
        CorruptCase {
            name: "index format version 1",
            corrupt: |block| block.uploaded[0].bytes[4] = 1,
            error: "invalid Prometheus TSDB block: index format version 1 is not supported; import \
             accepts versions 2 and 3, rewrite older blocks with a current Prometheus first",
        },
        CorruptCase {
            name: "tenant label of another tenant",
            corrupt: |block| {
                block
                    .labels
                    .insert("__org_id__".to_owned(), "tenant-b".to_owned());
            },
            error: "invalid Prometheus TSDB block: block external label __org_id__=\"tenant-b\" does \
             not match tenant \"tenant-a\"",
        },
        CorruptCase {
            name: "samples past maxTime",
            corrupt: |block| block.max_time = block.min_time + 3_600_000,
            error: "invalid Prometheus TSDB block: series {__name__=\"imported_counter_total\", \
             instance=\"i1\", job=\"fixture\"} has timestamp 1749999600000 outside the block \
             range [1749996000000, 1749999600000)",
        },
        CorruptCase {
            name: "declared chunk segment never uploaded",
            corrupt: |block| {
                block.declared.push(DeclaredUploadFile {
                    path: "chunks/000002".to_owned(),
                    size_bytes: 64,
                });
            },
            error: "invalid Prometheus TSDB block: the file chunks/000002 was not uploaded",
        },
    ];

    for case in cases {
        let krabka = Krabka::start().await?;
        let mut block = UploadBlock::fixture();
        (case.corrupt)(&mut block);

        let check_body = krabka.upload(FIXTURE_ULID, &block).await?;

        check!(
            check_body == json!({"result": "failed", "error": case.error}),
            "case: {}",
            case.name
        );
        check!(
            krabka.keys_under(MANIFEST_PREFIX).await?.is_empty(),
            "case: {}",
            case.name
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_repeated_file_path_is_read_once() -> TestResult {
    let krabka = Krabka::start().await?;
    krabka.upload(FIXTURE_ULID, &UploadBlock::fixture()).await?;
    let manifests = krabka.manifest_keys().await?;
    let mut repeated = UploadBlock::fixture();
    let chunks = DeclaredUploadFile {
        path: repeated.declared[1].path.clone(),
        size_bytes: repeated.declared[1].size_bytes,
    };
    repeated.declared.push(chunks);

    let check_body = krabka.upload(OTHER_ULID, &repeated).await?;

    check!(check_body == json!({"result": "complete", "existingBlock": FIXTURE_ULID}));
    check!(krabka.manifest_keys().await? == manifests);
    Ok(())
}

#[tokio::test]
async fn start_accepts_only_the_external_labels_that_mimir_accepts() -> TestResult {
    let cases = [
        ExternalLabelCase {
            name: "cluster",
            value: "prod",
            status: StatusCode::BAD_REQUEST,
            body: "unsupported external label: cluster\n",
        },
        ExternalLabelCase {
            name: "__tenant_id__",
            value: TENANT,
            status: StatusCode::BAD_REQUEST,
            body: "unsupported external label: __tenant_id__\n",
        },
        ExternalLabelCase {
            name: "__compactor_shard_id__",
            value: "0_of_4",
            status: StatusCode::BAD_REQUEST,
            body: "invalid __compactor_shard_id__ external label: \"0_of_4\"\n",
        },
        ExternalLabelCase {
            name: "__compactor_shard_id__",
            value: "5_of_4",
            status: StatusCode::BAD_REQUEST,
            body: "invalid __compactor_shard_id__ external label: \"5_of_4\"\n",
        },
        ExternalLabelCase {
            name: "__compactor_shard_id__",
            value: "+1_of_4",
            status: StatusCode::BAD_REQUEST,
            body: "invalid __compactor_shard_id__ external label: \"+1_of_4\"\n",
        },
        ExternalLabelCase {
            name: "__compactor_shard_id__",
            value: "1-of-4",
            status: StatusCode::BAD_REQUEST,
            body: "invalid __compactor_shard_id__ external label: \"1-of-4\"\n",
        },
        ExternalLabelCase {
            name: "__compactor_shard_id__",
            value: "4_of_4",
            status: StatusCode::OK,
            body: "",
        },
        ExternalLabelCase {
            name: "__compactor_shard_id__",
            value: "",
            status: StatusCode::OK,
            body: "",
        },
        ExternalLabelCase {
            name: "__ingester_id__",
            value: "ingester-1",
            status: StatusCode::OK,
            body: "",
        },
        ExternalLabelCase {
            name: "__shard_id__",
            value: "1",
            status: StatusCode::OK,
            body: "",
        },
    ];

    for case in cases {
        let krabka = Krabka::start().await?;
        let mut block = UploadBlock::fixture();
        block
            .labels
            .insert(case.name.to_owned(), case.value.to_owned());

        let response = krabka
            .post(
                krabka.upload_url(FIXTURE_ULID, "start"),
                block.meta(FIXTURE_ULID),
            )
            .await?;

        check!(
            response.status == case.status && response.body == case.body,
            "label: {}={:?}",
            case.name,
            case.value
        );
    }
    Ok(())
}

#[tokio::test]
async fn accepted_external_labels_are_not_added_to_the_imported_series() -> TestResult {
    let krabka = Krabka::start().await?;
    let mut block = UploadBlock::fixture();
    block
        .labels
        .insert("__compactor_shard_id__".to_owned(), "1_of_2".to_owned());

    let check_body = krabka.upload(FIXTURE_ULID, &block).await?;

    check!(check_body == json!({"result": "complete"}));
    check!(all_imported_samples(&krabka).await? == expected_series());
    Ok(())
}

/// An in-memory store whose ranged reads fail while [`Self::fail_ranges`] is
/// set.
#[derive(Debug, Default)]
struct RangeFailingStore {
    inner: InMemory,
    fail_ranges: AtomicBool,
}

impl std::fmt::Display for RangeFailingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RangeFailingStore")
    }
}

krabka_blockstore::delegate_object_store! {
    RangeFailingStore => inner;
    forward [put_opts, put_multipart_opts, delete_stream, list, list_with_delimiter, copy_opts];

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.range.is_some() && self.fail_ranges.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "RangeFailingStore",
                source: "the ranged read fails in the test".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }
}

// Starts the fixture upload and sends every file, without finishing it.
async fn start_and_upload_files(krabka: &Krabka, block: &UploadBlock) -> TestResult {
    let response = krabka
        .post(
            krabka.upload_url(FIXTURE_ULID, "start"),
            block.meta(FIXTURE_ULID),
        )
        .await?;
    assert!(
        response.status == StatusCode::OK,
        "start: {}",
        response.body
    );
    for file in &block.uploaded {
        let url = format!(
            "{}?path={}",
            krabka.upload_url(FIXTURE_ULID, "files"),
            encode(&file.path)
        );
        let response = krabka.post(url, file.bytes.clone()).await?;
        assert!(
            response.status == StatusCode::OK,
            "file {}: {}",
            file.path,
            response.body
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_store_failure_on_the_index_probe_leaves_the_upload_validating() -> TestResult {
    let store = Arc::new(RangeFailingStore::default());
    let krabka = Krabka::start_with(store.clone()).await?;
    let block = UploadBlock::fixture();
    start_and_upload_files(&krabka, &block).await?;
    store.fail_ranges.store(true, Ordering::SeqCst);

    let failed_response = krabka
        .post(krabka.upload_url(FIXTURE_ULID, "finish"), Vec::new())
        .await?;
    let during = krabka
        .get_json(&krabka.upload_url(FIXTURE_ULID, "check"), &[])
        .await?;
    store.fail_ranges.store(false, Ordering::SeqCst);
    let retry_response = krabka
        .post(krabka.upload_url(FIXTURE_ULID, "finish"), Vec::new())
        .await?;
    let after = krabka
        .get_json(&krabka.upload_url(FIXTURE_ULID, "check"), &[])
        .await?;

    check!(failed_response.status == StatusCode::INTERNAL_SERVER_ERROR);
    check!(during == json!({"result": "validating"}));
    check!(
        retry_response.status == StatusCode::OK,
        "retry: {}",
        retry_response.body
    );
    check!(after == json!({"result": "complete"}));
    check!(all_imported_samples(&krabka).await? == expected_series());
    Ok(())
}

#[tokio::test]
async fn a_query_during_a_stopped_import_reads_all_samples_or_none() -> TestResult {
    // The write where the process stops, and whether the samples are live
    // after the stop.
    let cases = [
        CrashPointCase {
            key_part: "/float.index",
            occurrence: 1,
            live: false,
        },
        CrashPointCase {
            key_part: "/native-histograms.index",
            occurrence: 1,
            live: false,
        },
        CrashPointCase {
            key_part: "/_published",
            occurrence: 1,
            live: false,
        },
        CrashPointCase {
            key_part: "/by-sha256/",
            occurrence: 2,
            live: true,
        },
    ];

    for case in cases {
        let store = Arc::new(CrashingStore::default());
        let krabka = Krabka::start_with(store.clone()).await?;
        let block = UploadBlock::fixture();
        start_and_upload_files(&krabka, &block).await?;
        store.crash_at(Some(CrashPoint {
            key_part: case.key_part.to_owned(),
            occurrence: case.occurrence,
        }));

        let stopped_response = krabka
            .post(krabka.upload_url(FIXTURE_ULID, "finish"), Vec::new())
            .await?;
        let during = all_imported_samples(&krabka).await?;
        store.crash_at(None);
        let retry_response = krabka
            .post(krabka.upload_url(FIXTURE_ULID, "finish"), Vec::new())
            .await?;
        let after = krabka
            .get_json(&krabka.upload_url(FIXTURE_ULID, "check"), &[])
            .await?;

        let case_name = format!("{} #{}", case.key_part, case.occurrence);
        check!(
            stopped_response.status == StatusCode::INTERNAL_SERVER_ERROR,
            "case: {case_name}"
        );
        let expected = if case.live {
            expected_series()
        } else {
            Vec::new()
        };
        check!(during == expected, "case: {case_name}");
        check!(
            retry_response.status == StatusCode::OK,
            "case: {case_name}: {}",
            retry_response.body
        );
        check!(after == json!({"result": "complete"}), "case: {case_name}");
        check!(
            all_imported_samples(&krabka).await? == expected_series(),
            "case: {case_name}"
        );
    }
    Ok(())
}

async fn start_prometheus(
    files: &FixtureFiles,
) -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let block = format!("/prometheus/{FIXTURE_ULID}");
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        pinned_prometheus_image::pinned_prometheus_image()
            .with_exposed_port(PROMETHEUS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "Server is ready to receive web requests",
            ))
            .with_copy_to("/etc/prometheus/prometheus.yml", b"global: {}\n".to_vec())
            .with_copy_to(format!("{block}/meta.json"), files.meta.clone())
            .with_copy_to(format!("{block}/index"), files.index.clone())
            .with_copy_to(format!("{block}/chunks/000001"), files.chunks.clone())
            .with_copy_to(format!("{block}/tombstones"), files.tombstones.clone())
            // The copied block belongs to root.
            .with_user("root")
            .with_cmd([
                "--config.file=/etc/prometheus/prometheus.yml",
                "--storage.tsdb.path=/prometheus",
                &format!("--enable-feature={PROMETHEUS_FEATURES}"),
                // Retention counts back from the newest block, and the block
                // is from 2025.
                "--storage.tsdb.retention.time=1000d",
            ])
            .start(),
    )
    .await??)
}

/// One request of the differential: a path and its parameters.
struct Request {
    path: String,
    params: Vec<QueryParameter>,
}

struct CardinalityRequestCase {
    request: Request,
    expected: Value,
}

impl Request {
    fn new(path: impl Into<String>, params: Vec<QueryParameter>) -> Self {
        Self {
            path: path.into(),
            params,
        }
    }
}

/// The offsets from the block start at which the differential reads
/// `imported_deleted` near its tombstone, each with a 5m window.
///
/// The tombstone deletes `[1749997800000, 1749998700000]`. The series has
/// chunks that end at `1749997785000` and `1749999585000`. Prometheus v3.14.0
/// panics with "index out of range [2] with length 2" when a query window
/// `[mint, maxt]` meets four conditions. A chunk that the query reads starts
/// before `mint`, a chunk that it reads ends after `maxt`, the tombstone
/// starts after `mint`, and the tombstone ends at or after `maxt`. For an
/// instant query with a 5m lookback, a local Prometheus v3.14.0 failed at
/// each offset in `[300000, 1780000]` and `[1800000, 2080000]`, on a 5s grid.
/// `/api/v1/query`, `/api/v1/query_range` and `/api/v1/series` all fail so.
///
/// This is a Prometheus bug, not a malformed block. Prometheus wrote these
/// tombstones itself, through `delete_series`. The panic is in
/// `Intervals.Add`, at `tsdb/tombstones/tombstones.go:379` of
/// `github.com/prometheus/prometheus v0.314.0`. `blockBaseSeriesSet.Next`, at
/// `tsdb/querier.go:647`, calls it to trim the chunks to the window. The
/// front trim puts `[MinInt64, mint-1]` before the tombstone. Then the back
/// trim adds `[maxt+1, MaxInt64]`, and its open-ended branch sets
/// `maxi := len(in)`. But `maxi` counts from `mini`, which is 1 here, so
/// `in[maxi+mini-1]` is `in[2]`. Commit 80b7f73d26, in v2.45.0, added that
/// branch. v0.315.0 and the development branch of 2026-10-02 still have it.
///
/// The same Prometheus answered each offset here without the panic. Krabka
/// does not have the bug, so an in-process test compares windows that crash
/// Prometheus with the samples that the Prometheus TSDB library reads.
const TOMBSTONE_OFFSETS: [i64; 8] = [
    290_000, 1_790_000, 1_795_000, 2_100_000, 2_400_000, 2_700_000, 2_720_000, 3_000_000,
];

/// The query requests that the differential sends to both APIs.
fn differential_requests() -> Vec<Request> {
    let start = FIXTURE_MIN_TIME;
    let end = FIXTURE_MAX_TIME;
    // Each time names a feature of the block: plain samples, the first
    // samples after the tombstoned range, the counter stale marker, the
    // histogram stale markers, and the end of the block. Each window here
    // starts before the block or ends after the tombstone, so no window meets
    // the conditions of the Prometheus panic in `TOMBSTONE_OFFSETS`.
    let instants = [
        start + 100_000,
        start + 3_000_000,
        start + 3_600_000,
        start + 4_500_000,
        start + 4_510_000,
        start + 6_000_000,
        start + 6_300_000,
        end - 1,
    ];
    let instant_queries = [
        "imported_counter_total",
        "imported_gauge",
        "imported_deleted",
        "imported_hist",
        "imported_float_hist",
        "imported_nhcb",
        "rate(imported_counter_total[5m])",
        "increase(imported_counter_total[1h])",
        "resets(imported_counter_total[2h])",
        "rate(imported_hist[5m])",
        "histogram_count(rate(imported_hist[5m]))",
        "histogram_quantile(0.9, rate(imported_hist[5m]))",
        "histogram_quantile(0.5, imported_nhcb)",
        "histogram_sum(imported_float_hist)",
        "histogram_fraction(0, 1, imported_float_hist)",
        "sum by (instance) (imported_gauge)",
        "max_over_time(imported_gauge[10m])",
        "count_over_time(imported_deleted[30m])",
        r#"{__name__=~"imported_.+"}[2m]"#,
    ];
    let range_queries = [
        "imported_counter_total",
        "imported_gauge",
        "imported_deleted",
        "imported_hist",
        "imported_float_hist",
        "imported_nhcb",
        "rate(imported_counter_total[5m])",
        "histogram_count(imported_hist)",
    ];
    let mut requests = Vec::new();
    for time in instants {
        for query in instant_queries {
            requests.push(Request::new(
                "/api/v1/query",
                vec![
                    QueryParameter::new("query", query),
                    QueryParameter::new("time", seconds(time)),
                ],
            ));
        }
    }
    for query in range_queries {
        requests.push(Request::new(
            "/api/v1/query_range",
            vec![
                QueryParameter::new("query", query),
                QueryParameter::new("start", seconds(start)),
                QueryParameter::new("end", seconds(end)),
                QueryParameter::new("step", "60"),
            ],
        ));
    }
    let range = vec![
        QueryParameter::new("start", seconds(start)),
        QueryParameter::new("end", seconds(end)),
    ];
    let mut series = range.clone();
    series.push(QueryParameter::new(
        "match[]",
        r#"{__name__=~"imported_.+"}"#,
    ));
    requests.push(Request::new("/api/v1/series", series));
    requests.push(Request::new("/api/v1/labels", range.clone()));
    requests.push(Request::new("/api/v1/label/instance/values", range));
    requests.extend(tombstone_requests());
    requests.extend(histogram_requests());
    requests.extend(cardinality_requests());
    requests.extend(metadata_and_exemplar_requests());
    requests
}

fn instant(query: &str, time: i64) -> Request {
    Request::new(
        "/api/v1/query",
        vec![
            QueryParameter::new("query", query),
            QueryParameter::new("time", seconds(time)),
        ],
    )
}

fn range_query(query: &str, start: i64, end: i64, step: &str) -> Request {
    Request::new(
        "/api/v1/query_range",
        vec![
            QueryParameter::new("query", query),
            QueryParameter::new("start", seconds(start)),
            QueryParameter::new("end", seconds(end)),
            QueryParameter::new("step", step),
        ],
    )
}

fn matched(path: &str, selector: &str, start: i64, end: i64) -> Request {
    Request::new(
        path,
        vec![
            QueryParameter::new("match[]", selector),
            QueryParameter::new("start", seconds(start)),
            QueryParameter::new("end", seconds(end)),
        ],
    )
}

/// Requests for `imported_deleted` before, in and after its tombstone, at
/// windows that do not hit the Prometheus panic of [`TOMBSTONE_OFFSETS`].
fn tombstone_requests() -> Vec<Request> {
    let start = FIXTURE_MIN_TIME;
    let queries = [
        "imported_deleted",
        "timestamp(imported_deleted)",
        "rate(imported_deleted[5m])",
        "count_over_time(imported_deleted[5m])",
        "last_over_time(imported_deleted[5m])",
        "imported_deleted[5m]",
    ];
    let mut requests = Vec::new();
    for offset in TOMBSTONE_OFFSETS {
        for query in queries {
            requests.push(instant(query, start + offset));
        }
    }
    // Each window starts after the first chunk ends, at 1749997785000.
    for window in [
        RangeWindow {
            from: 2_100_000,
            to: 7_200_000,
            step: "60",
        },
        RangeWindow {
            from: 2_100_000,
            to: 2_600_000,
            step: "15",
        },
    ] {
        for query in &queries[..4] {
            requests.push(range_query(
                query,
                start + window.from,
                start + window.to,
                window.step,
            ));
        }
    }
    for window in [
        SeriesWindow {
            from: 1_900_000,
            to: 2_600_000,
        },
        SeriesWindow {
            from: 2_100_000,
            to: 2_600_000,
        },
    ] {
        for path in [
            "/api/v1/series",
            "/api/v1/labels",
            "/api/v1/label/job/values",
        ] {
            requests.push(matched(
                path,
                "imported_deleted",
                start + window.from,
                start + window.to,
            ));
        }
    }
    requests
}

/// Histogram functions over the integer, float and custom-bucket histograms.
fn histogram_requests() -> Vec<Request> {
    let start = FIXTURE_MIN_TIME;
    let queries = [
        "histogram_avg(imported_hist)",
        "histogram_stddev(imported_float_hist)",
        "histogram_stdvar(imported_nhcb)",
        "histogram_count(imported_nhcb)",
        "histogram_sum(imported_nhcb)",
        "histogram_quantile(0.5, imported_float_hist)",
        "histogram_fraction(0, 0.5, imported_nhcb)",
        "histogram_quantile(0.99, sum(rate(imported_nhcb[5m])))",
        "sum(imported_hist)",
        "increase(imported_hist[30m])",
    ];
    let mut requests = Vec::new();
    for time in [start + 100_000, start + 4_510_000, FIXTURE_MAX_TIME - 1] {
        for query in queries {
            requests.push(instant(query, time));
        }
    }
    for query in [
        "histogram_sum(imported_float_hist)",
        "histogram_quantile(0.5, imported_nhcb)",
    ] {
        requests.push(range_query(query, start, FIXTURE_MAX_TIME, "60"));
    }
    requests
}

/// The cardinality that Prometheus answers from a block.
///
/// Prometheus gives `/api/v1/status/tsdb` from the head only, so it reports
/// no series for a served block. Krabka gives that route from the blocks
/// inside its lookback, and its `headStats` has other fields, so the
/// differential leaves the route out. `count` over the series and the
/// discovery routes give the cardinality that both read from the block.
fn cardinality_requests() -> Vec<Request> {
    let start = FIXTURE_MIN_TIME;
    let end = FIXTURE_MAX_TIME;
    let mut requests = Vec::new();
    for time in [start + 100_000, start + 4_510_000, end - 1] {
        for query in [
            r#"count by (__name__) ({__name__=~"imported_.+"})"#,
            r#"count by (job) ({__name__=~"imported_.+"})"#,
            "count(count by (instance) (imported_gauge))",
        ] {
            requests.push(instant(query, time));
        }
    }
    requests.push(range_query(
        r#"count({__name__=~"imported_.+"})"#,
        start,
        end,
        "300",
    ));
    requests.push(matched("/api/v1/labels", "imported_deleted", start, end));
    requests.push(matched(
        "/api/v1/label/__name__/values",
        "imported_gauge",
        start,
        end,
    ));
    requests.push(Request::new(
        "/api/v1/label/__name__/values",
        vec![
            QueryParameter::new("start", seconds(start)),
            QueryParameter::new("end", seconds(end)),
        ],
    ));
    requests
}

/// A TSDB block holds no metric metadata and no exemplars, so Prometheus
/// serving the block answers both routes empty.
fn metadata_and_exemplar_requests() -> Vec<Request> {
    let range = [
        QueryParameter::new("start", seconds(FIXTURE_MIN_TIME)),
        QueryParameter::new("end", seconds(FIXTURE_MAX_TIME)),
    ];
    let mut requests = vec![
        Request::new("/api/v1/metadata", Vec::new()),
        Request::new(
            "/api/v1/metadata",
            vec![QueryParameter::new("metric", "imported_gauge")],
        ),
    ];
    for selector in ["imported_hist", r#"{__name__=~"imported_.+"}"#] {
        let mut params = vec![QueryParameter::new("query", selector)];
        params.extend(range.iter().cloned());
        requests.push(Request::new("/api/v1/query_exemplars", params));
    }
    requests
}

/// Sends each differential request to Prometheus at `prometheus_base` and to
/// `krabka`, and returns a description of each request whose answers differ.
async fn differential_mismatches(
    prometheus_base: &str,
    krabka: &Krabka,
) -> TestResult<Vec<String>> {
    let mut mismatches = Vec::new();
    for request in differential_requests() {
        let path = request.path;
        let params = request.params;
        let upstream = krabka
            .get_json(&format!("{prometheus_base}{path}"), &params)
            .await?;
        let imported = krabka
            .get_json(&format!("{}{path}", krabka.base), &params)
            .await?;
        if !diff_corpus::queries_equal(&upstream, &imported) {
            mismatches.push(format!(
                "{path} {params:?}\nprometheus: {upstream}\nkrabka: {imported}"
            ));
        }
    }
    Ok(mismatches)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn an_imported_block_answers_queries_as_prometheus_serving_the_block() -> TestResult {
    let files = fixture_files();
    let prometheus = start_prometheus(&files).await?;
    let prometheus_base = format!(
        "http://127.0.0.1:{}",
        prometheus.get_host_port_ipv4(PROMETHEUS_PORT.tcp()).await?
    );
    let krabka = Krabka::start().await?;
    let check_body = krabka
        .upload(FIXTURE_ULID, &UploadBlock::new(files))
        .await?;
    assert!(check_body == json!({"result": "complete"}));

    let mismatches = differential_mismatches(&prometheus_base, &krabka).await?;

    assert!(
        mismatches.is_empty(),
        "{} requests differ:\n{}",
        mismatches.len(),
        mismatches.join("\n\n")
    );
    Ok(())
}

#[tokio::test]
async fn every_differential_request_succeeds_on_the_imported_block() -> TestResult {
    let krabka = Krabka::start().await?;
    krabka.upload(FIXTURE_ULID, &UploadBlock::fixture()).await?;

    let mut failures = Vec::new();
    for request in differential_requests() {
        let path = request.path;
        let params = request.params;
        let response = krabka
            .get_json(&format!("{}{path}", krabka.base), &params)
            .await?;
        if response["status"] != "success" {
            failures.push(format!("{path} {params:?}: {response}"));
        }
    }

    check!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn an_imported_block_has_no_metadata_and_no_exemplars() -> TestResult {
    // The answers of Prometheus v3.14.0 serving the fixture block, with the
    // differential's feature flags.
    let empty_metadata = json!({"status": "success", "data": {}});
    let empty_exemplars = json!({"status": "success", "data": []});
    let krabka = Krabka::start().await?;
    krabka.upload(FIXTURE_ULID, &UploadBlock::fixture()).await?;

    for request in metadata_and_exemplar_requests() {
        let path = request.path;
        let params = request.params;
        let response = krabka
            .get_json(&format!("{}{path}", krabka.base), &params)
            .await?;
        let expected = if path == "/api/v1/metadata" {
            &empty_metadata
        } else {
            &empty_exemplars
        };

        check!(&response == expected, "{path} {params:?}");
    }
    Ok(())
}

/// The samples of `imported_deleted` that the Prometheus TSDB library reads,
/// in the left-open window `(time - 5m, time]` of a range selector.
fn expected_deleted_window(time: i64) -> Vec<FloatBitsSample> {
    let mut deleted = BTreeMap::new();
    deleted.insert("__name__".to_owned(), "imported_deleted".to_owned());
    deleted.insert("job".to_owned(), "fixture".to_owned());
    expected_series()
        .into_iter()
        .find(|series| series.labels == deleted)
        .expect("the fixture holds imported_deleted")
        .floats
        .into_iter()
        .filter(|sample| sample.timestamp_ms > time - 300_000 && sample.timestamp_ms <= time)
        .collect()
}

#[tokio::test]
async fn the_tombstoned_series_matches_prometheus_where_prometheus_panics() -> TestResult {
    // Offsets from the block start whose 5m window makes Prometheus v3.14.0
    // panic, as `TOMBSTONE_OFFSETS` explains: before the tombstone, at its
    // start, and inside it. The Prometheus TSDB library reads the block
    // without the PromQL trim, so its samples are the reference here.
    let offsets = [
        300_000, 1_000_000, 1_780_000, 1_800_000, 1_900_000, 2_080_000,
    ];
    let krabka = Krabka::start().await?;
    krabka.upload(FIXTURE_ULID, &UploadBlock::fixture()).await?;

    for offset in offsets {
        let time = FIXTURE_MIN_TIME + offset;
        let response = krabka
            .get_json(
                &format!("{}/api/v1/query", krabka.base),
                &[
                    QueryParameter::new("query", "imported_deleted[5m]"),
                    QueryParameter::new("time", seconds(time)),
                ],
            )
            .await?;
        let expected = expected_deleted_window(time);
        let actual: Vec<FloatBitsSample> = matrix_series(&response)
            .into_iter()
            .flat_map(|series| series.floats)
            .collect();

        check!(actual == expected, "offset {offset}");
    }
    Ok(())
}

/// The answers of the cardinality routes when Krabka reads no series.
fn empty_cardinality(path: &str) -> Value {
    match path {
        "/api/v1/cardinality/label_names" => {
            json!({"label_values_count_total": 0, "label_names_count": 0, "cardinality": []})
        }
        "/api/v1/cardinality/label_values" => json!({"series_count_total": 0, "labels": []}),
        _ => json!({"data": []}),
    }
}

#[tokio::test]
async fn cardinality_routes_count_the_imported_block_inside_the_lookback() -> TestResult {
    // These are Grafana Mimir routes. Mimir reads them from the ingester head
    // only, so after the same upload Mimir gives no series. These routes take
    // no time range, so Krabka reads the blocks inside the unbounded
    // compatibility lookback. With the default lookback of one hour, the 2025
    // block is outside it, and Krabka gives no series as Mimir does. With a
    // lookback that reaches the block, Krabka counts its series. The values
    // are hand-computed from the seven series of the fixture.
    let only_job = |name: &str| json!({"__name__": name, "job": "fixture"});
    let with_instance = |name: &str, instance: &str| json!({"__name__": name, "instance": instance, "job": "fixture"});
    let histogram_metric = |metric: &str, buckets: u64| {
        json!({
            "metric": metric, "series_count": 1, "bucket_count": buckets,
            "avg_bucket_count": f64::from(u32::try_from(buckets).expect("a small count")),
            "min_bucket_count": buckets, "max_bucket_count": buckets,
        })
    };
    let selector = vec![QueryParameter::new("selector", r#"{job="fixture"}"#)];
    let cases = [
        CardinalityRequestCase {
            request: Request::new("/api/v1/cardinality/label_names", Vec::new()),
            expected: json!({
                "label_values_count_total": 9,
                "label_names_count": 3,
                "cardinality": [
                    {"label_name": "__name__", "label_values_count": 6},
                    {"label_name": "instance", "label_values_count": 2},
                    {"label_name": "job", "label_values_count": 1},
                ],
            }),
        },
        CardinalityRequestCase {
            request: Request::new(
                "/api/v1/cardinality/label_values",
                vec![
                    QueryParameter::new("label_names[]", "instance"),
                    QueryParameter::new("label_names[]", "job"),
                ],
            ),
            expected: json!({
                "series_count_total": 7,
                "labels": [
                    {
                        "label_name": "job",
                        "label_values_count": 1,
                        "series_count": 7,
                        "cardinality": [{"label_value": "fixture", "series_count": 7}],
                    },
                    {
                        "label_name": "instance",
                        "label_values_count": 2,
                        "series_count": 3,
                        "cardinality": [
                            {"label_value": "i1", "series_count": 2},
                            {"label_value": "zürich", "series_count": 1},
                        ],
                    },
                ],
            }),
        },
        CardinalityRequestCase {
            request: Request::new("/api/v1/cardinality/active_series", selector.clone()),
            expected: json!({"data": [
                with_instance("imported_counter_total", "i1"),
                only_job("imported_deleted"),
                only_job("imported_float_hist"),
                with_instance("imported_gauge", "i1"),
                with_instance("imported_gauge", "zürich"),
                only_job("imported_hist"),
                only_job("imported_nhcb"),
            ]}),
        },
        CardinalityRequestCase {
            request: Request::new(
                "/api/v1/cardinality/active_native_histogram_metrics",
                selector,
            ),
            // The buckets of the last sample of each series: two positive,
            // four positive and two negative, and four custom buckets.
            expected: json!({"data": [
                histogram_metric("imported_float_hist", 2),
                histogram_metric("imported_hist", 6),
                histogram_metric("imported_nhcb", 4),
            ]}),
        },
    ];
    let LookbackPair { recent, reaching } = LookbackPair::with_fixture_uploaded().await?;

    for case in cases {
        let path = case.request.path;
        let params = case.request.params;
        let outside = recent
            .get_json(&format!("{}{path}", recent.base), &params)
            .await?;
        let inside = reaching
            .get_json(&format!("{}{path}", reaching.base), &params)
            .await?;

        check!(outside == empty_cardinality(&path), "{path}");
        check!(inside == case.expected, "{path}");
    }
    Ok(())
}

/// The `/api/v1/status/tsdb` data of `krabka`, without `headStats`.
async fn tsdb_status_lists(krabka: &Krabka) -> TestResult<Value> {
    let response = krabka
        .get_json(&format!("{}/api/v1/status/tsdb", krabka.base), &[])
        .await?;
    assert!(response["status"] == "success", "{response}");
    let mut data = response["data"].clone();
    data.as_object_mut()
        .expect("the TSDB status is an object")
        .remove("headStats");
    Ok(data)
}

#[tokio::test]
async fn tsdb_status_counts_the_imported_block_inside_the_lookback() -> TestResult {
    // Prometheus gives these lists from its head only. Prometheus v3.14.0
    // serving the fixture block gives four empty lists. Krabka gives the
    // same with the default lookback of one hour, because the 2025 block is
    // outside it. With a lookback that reaches the block, Krabka gives the
    // lists that Prometheus gives when the generator writes the same seven
    // series into its head. The one difference is `memoryInBytesByLabelName`:
    // Prometheus counts one more byte for the length of each label name and
    // value, and gives 181, 84 and 41.
    //
    // The differential does not compare this route. `headStats` describes the
    // Prometheus head, which Krabka does not have, and Krabka gives
    // `numSamples` where Prometheus gives `numLabelPairs`.
    let stat = |name: &str, value: u64| json!({"name": name, "value": value});
    let empty = json!({
        "seriesCountByMetricName": [],
        "labelValueCountByLabelName": [],
        "memoryInBytesByLabelName": [],
        "seriesCountByLabelValuePair": [],
    });
    let counted = json!({
        "seriesCountByMetricName": [
            stat("imported_gauge", 2),
            stat("imported_counter_total", 1),
            stat("imported_deleted", 1),
            stat("imported_float_hist", 1),
            stat("imported_hist", 1),
            stat("imported_nhcb", 1),
        ],
        "labelValueCountByLabelName": [
            stat("__name__", 6),
            stat("instance", 2),
            stat("job", 1),
        ],
        // The UTF-8 bytes of each label name and value, over the series.
        "memoryInBytesByLabelName": [
            stat("__name__", 167),
            stat("job", 70),
            stat("instance", 35),
        ],
        "seriesCountByLabelValuePair": [
            stat("job=fixture", 7),
            stat("__name__=imported_gauge", 2),
            stat("instance=i1", 2),
            stat("__name__=imported_counter_total", 1),
            stat("__name__=imported_deleted", 1),
            stat("__name__=imported_float_hist", 1),
            stat("__name__=imported_hist", 1),
            stat("__name__=imported_nhcb", 1),
            stat("instance=zürich", 1),
        ],
    });
    let LookbackPair { recent, reaching } = LookbackPair::with_fixture_uploaded().await?;

    check!(tsdb_status_lists(&recent).await? == empty);
    check!(tsdb_status_lists(&reaching).await? == counted);
    Ok(())
}

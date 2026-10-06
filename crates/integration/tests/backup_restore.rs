// The PromQL router's query future holds DataFusion state. Its axum `Send`
// check needs the same trait recursion depth as the other router suites.
#![recursion_limit = "512"]
//! A consistent backup of a whole deployment, restored into an empty one.
//!
//! The suite boots a real broker in-process and drives every signal through
//! its production write path: the distributor's Kafka sink, the WAL topic,
//! the block builder's consumer loop, and an object store. It also writes the
//! state that is not a block: Mimir rule groups and a metric erasure request
//! in the metrics bucket, ruler state on its compacted topic, Loki rule groups
//! and a log delete request under the logs `data-root`, and profile symbols.
//! A metric exemplar, a log line and a profile sample carry the id of the
//! stored trace.
//!
//! The suite then seals one deployment cut with `krabka-blockstore`'s
//! recovery API and `krabka-observability`'s Kafka broker reader, copies the
//! stopped broker's log directory, and restores both halves into empty stores
//! and a broker that starts on the copy. Every query answer before the cut
//! must equal the answer after the restore. Last, more records go through the
//! restored broker, and the suite checks that each block builder resumes at
//! the recorded offset: every record is in exactly one block.
//!
//! A second test imports a Prometheus TSDB block through the Mimir
//! block-upload routes before the cut. After the restore, the import record
//! and the ULID binding are in the metrics bucket, and an upload of the same
//! block gets the duplicate answer and adds no sample.
//!
//! The suite writes the sealed cut, the audit and restore reports, and the
//! query answers, with a `SHA256SUMS` manifest of those files, into the
//! directory that `KRABKA_RECOVERY_EVIDENCE_DIR` names. Under Bazel it falls
//! back to `TEST_UNDECLARED_OUTPUTS_DIR`, and the `recovery` qualification
//! gate archives those outputs with the test log.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    BlockStore, BrokerState as _, DeploymentPart, ERASURE_REQUEST_PREFIX, ErasureRequest,
    LabelMatcher, MatchOp, ProfileIndex, TraceIndex, audit_deployment_backup, backup_deployment,
    labels, put_erasure_request, restore_deployment_backup,
};
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};
use krabka_client_producer::Producer;
use krabka_metrics::{
    MetricsCompactorConfig, SamplePayload, TsdbBlockFiles, WalExemplar, WalRecord,
    distributor::{KafkaSink as MetricsKafkaSink, WalSink as _},
    list_compaction_manifests,
    metrics::ServiceMetrics as MetricsServiceMetrics,
    partition_key, run_compactor_consumer_loop, tsdb_block_sha256,
};
use krabka_metrics_service::{
    KafkaRecordingRuleWalSink, KafkaRulerStateSink, MimirTenantAdminState,
    RefreshingMetricBlockStore, RulerStateWalRecord, blockstore_prometheus_router,
    mimir_tenant_admin_router,
};
use krabka_observability::{
    KafkaLogWalConsumer, KafkaLogWalSink, LogWalSink as _, QuerierIndexSource, Role, ServiceConfig,
    ServiceDependencies, WalLogRecord, build_service_router,
    recovery_cut::{KafkaBrokerState, deployment_drained_groups},
    run_compactor_until_idle,
    server_security::{ServerSecurity, authenticate_requests},
    topic_contract::{
        ALL_TOPICS, LOGS_WAL_TOPIC, METRICS_RULER_STATE_TOPIC, METRICS_WAL_TOPIC,
        PROFILES_WAL_TOPIC, TRACES_WAL_TOPIC, TopicSettings, provision_topics,
    },
    wal_consumer_metrics::WalConsumerMetrics,
};
use krabka_profiles::{
    ProfileRecord, WalFunction, WalLocation, WalMapping, WalSample, WalSymbolSet,
    blockbuilder::{BlockBuilderConfig as ProfilesBlockBuilderConfig, run_with_config},
    cold_store::ColdProfileStore,
    distributor::{KafkaSink as ProfilesKafkaSink, WalSink as _},
    limits::Limits,
    query::{self, QuerierState},
};
use krabka_promql::{
    InMemoryMetricStore, RecordingRuleWalSink as _, RulerAlertStateRecord, RulerGroupStateRecord,
    RulerStateSink as _, WalHead,
};
use krabka_traceql::{EngineOpts as TraceqlOpts, TraceqlEngine};
use krabka_traces::{
    Span, SpanKind, SpanRecord, StatusCode as SpanStatus,
    blockbuilder::{BlockBuilderConfig as TracesBlockBuilderConfig, BlockBuilderConsumer},
    distributor::{KafkaSink as TracesKafkaSink, WalSink as _},
    metrics::ServiceMetrics as TracesServiceMetrics,
    querier::store::KrabkaSpanStore,
};
use krabka_units::{Time, convert::TimeExt as _, millis};
use object_store::{ObjectStore, ObjectStoreExt as _, local::LocalFileSystem, memory::InMemory};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;
use tsdb_fixture::{FIXTURE_MAX_TIME, FIXTURE_MIN_TIME, FIXTURE_ULID, fixture_files};

#[path = "../../metrics/tests/support/tsdb_fixture.rs"]
mod tsdb_fixture;

const TENANTS: [&str; 2] = ["tenant-a", "tenant-b"];
/// The trace that the exemplar, the log line, and the profile sample name.
const TRACE_ID: [u8; 16] = [
    0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36,
];
const BASE_MS: i64 = 1_700_000_000_000;
const LOGS_INDEX_PREFIX: &str = "logs";
const TRACE_INDEX_KEY: &str = "index/traces.json";
const PROFILE_INDEX_KEY: &str = "index/profiles.json";
const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const FRAME: &str = "checkout.handle";
const METRICS_GROUP: &str = "krabka-metrics-block-builder";
const LOGS_GROUP: &str = "krabka-observability-block-builder";
const TRACES_GROUP: &str = "krabka-traces-block-builder";
const PROFILES_GROUP: &str = "krabka-profiles-block-builder";
const BROKER_DEADLINE: Duration = Duration::from_secs(30);
const ALERT_RULE: &str = "BackupProbe\nbackup_probe > 0";
/// The object prefix of Mimir block uploads in the metrics bucket.
const UPLOAD_PREFIX: &str = "mimir-block-uploads";
/// A second ULID for an upload of the same Prometheus block content.
const OTHER_ULID: &str = "01M3MJXM7R4M5X4Q4CKHW5Q8N1";

/// One deployment: a broker log directory, a bucket for each signal, and the
/// local `data-root` that the logs block builder and querier share.
struct Deployment {
    broker_dir: tempfile::TempDir,
    metrics: Arc<dyn ObjectStore>,
    traces: Arc<dyn ObjectStore>,
    profiles: Arc<dyn ObjectStore>,
    logs_dir: tempfile::TempDir,
    logs: Arc<dyn ObjectStore>,
    logs_state_dir: tempfile::TempDir,
    logs_state: Arc<dyn ObjectStore>,
}

impl Deployment {
    fn empty() -> Self {
        let logs_dir = tempfile::tempdir().expect("logs bucket");
        let logs_state_dir = tempfile::tempdir().expect("logs data root");
        Self {
            broker_dir: tempfile::tempdir().expect("broker dir"),
            metrics: Arc::new(InMemory::new()),
            traces: Arc::new(InMemory::new()),
            profiles: Arc::new(InMemory::new()),
            logs: Arc::new(LocalFileSystem::new_with_prefix(logs_dir.path()).expect("logs store")),
            logs_state: Arc::new(
                LocalFileSystem::new_with_prefix(logs_state_dir.path()).expect("state store"),
            ),
            logs_dir,
            logs_state_dir,
        }
    }

    /// The parts of a deployment cut. The two logs roles share one
    /// `data-root` here, as an all-in-one logs role does, so it is one part.
    fn parts(&self) -> Vec<DeploymentPart> {
        [
            ("logs", &self.logs),
            ("logs-data-root", &self.logs_state),
            ("metrics", &self.metrics),
            ("profiles", &self.profiles),
            ("traces", &self.traces),
        ]
        .into_iter()
        .map(|(name, store)| DeploymentPart {
            name: name.into(),
            store: Arc::clone(store),
        })
        .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_deployment_restores_every_tenant_and_signal_and_resumes_ingest() {
    let evidence = Evidence::from_env();
    let live = Deployment::empty();
    let broker = start_broker(live.broker_dir.path(), BootstrapMode::Bootstrap).await;
    let bootstrap = broker.listen_addr().to_string();
    provision_topics(
        &bootstrap,
        &ALL_TOPICS,
        &TopicSettings::single_broker(),
        None,
    )
    .await
    .expect("provision topics");
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(&bootstrap)
            .build()
            .await
            .expect("producer"),
    );

    // --- write every signal and every kind of state, then build blocks ----
    for tenant in TENANTS {
        produce_generation(&producer, &bootstrap, tenant, 0).await;
    }
    write_state(&live, &bootstrap).await;
    build_all_blocks(&live, &bootstrap).await;

    let before = query_everything(&live, &bootstrap).await;
    check_expected_answers(&before, 1);

    // --- seal the cut: quiesced writers, drained block builders -----------
    let broker_state = cut_broker_state(&bootstrap);
    let backup: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let backup_report = backup_deployment(
        &broker_state,
        &backup,
        &krabka_blockstore::DeploymentBackupPlan {
            cut_id: "cut-backup-restore".into(),
            broker_capture: "in-process-broker-log-dir".into(),
            drained_groups: deployment_drained_groups(),
            parts: live.parts(),
            omitted_parts: Vec::new(),
        },
    )
    .await
    .expect("seal the deployment cut");
    evidence.write("backup-report.json", &backup_report);
    let audit = audit_deployment_backup(&backup)
        .await
        .expect("audit the set");
    assert!(
        audit
            .parts
            .values()
            .all(krabka_blockstore::AuditReport::is_clean)
    );
    evidence.write("audit-report.json", &audit);

    // --- the broker half: stop it, copy its log directory -----------------
    drop(producer);
    broker.shutdown().await;
    let restored = Deployment::empty();
    copy_dir(live.broker_dir.path(), restored.broker_dir.path());

    // --- the empty deployment ---------------------------------------------
    let restored_broker = start_broker(restored.broker_dir.path(), BootstrapMode::Rejoin).await;
    wait_for_ready(&restored_broker).await;
    let restored_bootstrap = restored_broker.listen_addr().to_string();
    let restore_report = restore_deployment_backup(
        &backup,
        &cut_broker_state(&restored_bootstrap),
        &restored.parts(),
    )
    .await
    .expect("restore the deployment");
    check!(restore_report.broker == backup_report.cut.broker);
    evidence.write("restore-report.json", &restore_report);

    let after = query_everything(&restored, &restored_bootstrap).await;
    for (name, answer) in &before {
        check!(
            after.get(name) == Some(answer),
            "{name} survives the restore"
        );
    }
    check!(after.len() == before.len());
    evidence.write("query-before.json", &before);
    evidence.write("query-after-restore.json", &after);
    evidence.write(
        "backup-query-invariance.json",
        &json!({"cases":before.iter().map(|(name, answer)| json!({
        "id":name, "status":if after.get(name) == Some(answer) {"matched"} else {"mismatch"},
        "before":answer, "restored":after.get(name),
    })).collect::<Vec<_>>()}),
    );

    // --- ingest resumes from the cut --------------------------------------
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(&restored_bootstrap)
            .build()
            .await
            .expect("producer"),
    );
    for tenant in TENANTS {
        produce_generation(&producer, &restored_bootstrap, tenant, 1).await;
    }
    build_all_blocks(&restored, &restored_bootstrap).await;
    let resumed = query_everything(&restored, &restored_bootstrap).await;
    check_expected_answers(&resumed, 2);
    evidence.write("query-after-resumed-ingest.json", &resumed);

    // No block covers a WAL offset that a pre-cut block covers, so nothing
    // was published twice, and every offset up to the new end is covered, so
    // nothing was lost.
    let coverage = wal_coverage(&restored).await;
    let resumed_snapshot = cut_broker_state(&restored_bootstrap)
        .capture()
        .await
        .expect("read the resumed broker");
    for (signal, topic) in [
        ("logs", LOGS_WAL_TOPIC),
        ("metrics", METRICS_WAL_TOPIC),
        ("profiles", PROFILES_WAL_TOPIC),
        ("traces", TRACES_WAL_TOPIC),
    ] {
        let end = resumed_snapshot
            .wal_offsets
            .iter()
            .find(|offset| offset.topic == topic)
            .expect("WAL partition")
            .next_offset;
        let cut = backup_report
            .cut
            .broker
            .wal_offsets
            .iter()
            .find(|offset| offset.topic == topic)
            .expect("WAL partition at the cut")
            .next_offset;
        check!(cut < end, "{signal}: new records reached the restored WAL");
        let records = record_offsets(&restored_bootstrap, topic, end).await;
        check!(
            covers_exactly_once(&coverage[signal], &records, end),
            "{signal}: every WAL record is in exactly one block of each series: {:?}, records {records:?}",
            coverage[signal]
        );
    }
    evidence.write("wal-coverage.json", &coverage);
    evidence.seal();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_restored_from_another_time_is_refused_before_any_write() {
    let live = Deployment::empty();
    let broker = start_broker(live.broker_dir.path(), BootstrapMode::Bootstrap).await;
    let bootstrap = broker.listen_addr().to_string();
    provision_topics(
        &bootstrap,
        &ALL_TOPICS,
        &TopicSettings::single_broker(),
        None,
    )
    .await
    .expect("provision topics");
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(&bootstrap)
            .build()
            .await
            .expect("producer"),
    );
    produce_generation(&producer, &bootstrap, TENANTS[0], 0).await;
    build_all_blocks(&live, &bootstrap).await;

    // A snapshot of the broker from before the cut.
    broker.shutdown().await;
    let stale_dir = tempfile::tempdir().expect("stale broker dir");
    copy_dir(live.broker_dir.path(), stale_dir.path());

    let broker = start_broker(live.broker_dir.path(), BootstrapMode::Rejoin).await;
    wait_for_ready(&broker).await;
    let bootstrap = broker.listen_addr().to_string();
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(&bootstrap)
            .build()
            .await
            .expect("producer"),
    );
    produce_generation(&producer, &bootstrap, TENANTS[1], 0).await;
    build_all_blocks(&live, &bootstrap).await;

    let backup: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    backup_deployment(
        &cut_broker_state(&bootstrap),
        &backup,
        &krabka_blockstore::DeploymentBackupPlan {
            cut_id: "cut-stale-broker".into(),
            broker_capture: "in-process-broker-log-dir".into(),
            drained_groups: deployment_drained_groups(),
            parts: live.parts(),
            omitted_parts: Vec::new(),
        },
    )
    .await
    .expect("seal the deployment cut");
    drop(producer);
    broker.shutdown().await;

    let restored = Deployment::empty();
    copy_dir(stale_dir.path(), restored.broker_dir.path());
    let stale_broker = start_broker(restored.broker_dir.path(), BootstrapMode::Rejoin).await;
    wait_for_ready(&stale_broker).await;
    let error = restore_deployment_backup(
        &backup,
        &cut_broker_state(&stale_broker.listen_addr().to_string()),
        &restored.parts(),
    )
    .await
    .expect_err("a broker of another time is refused");

    assert!(matches!(
        error,
        krabka_blockstore::RecoveryError::BrokerMismatch { .. }
    ));
    for part in restored.parts() {
        check!(
            list_paths(&part.store).await.is_empty(),
            "{} stays empty",
            part.name
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_deployment_keeps_a_tsdb_import_and_does_not_import_it_again() {
    let live = Deployment::empty();
    let broker = start_broker(live.broker_dir.path(), BootstrapMode::Bootstrap).await;
    let bootstrap = broker.listen_addr().to_string();
    provision_topics(
        &bootstrap,
        &ALL_TOPICS,
        &TopicSettings::single_broker(),
        None,
    )
    .await
    .expect("provision topics");

    let imported = upload_fixture_block(&mimir_upload_router(&live.metrics), FIXTURE_ULID).await;
    check!(imported == json!({"result": "complete"}));
    let records = objects_under(&live.metrics, UPLOAD_PREFIX).await;
    check!(records.keys().cloned().collect::<BTreeSet<_>>() == expected_upload_keys());
    let samples = imported_sample_count(&live.metrics).await;
    check!(samples == json!([FIXTURE_MAX_TIME / 1_000, "3296"]));
    let manifests = manifest_keys(&live.metrics).await;
    check!(
        manifests.len() == 2,
        "a float and a native-histogram manifest"
    );

    let backup: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    backup_deployment(
        &cut_broker_state(&bootstrap),
        &backup,
        &krabka_blockstore::DeploymentBackupPlan {
            cut_id: "cut-tsdb-import".into(),
            broker_capture: "in-process-broker-log-dir".into(),
            drained_groups: deployment_drained_groups(),
            parts: live.parts(),
            omitted_parts: Vec::new(),
        },
    )
    .await
    .expect("seal the deployment cut");
    broker.shutdown().await;
    let restored = Deployment::empty();
    copy_dir(live.broker_dir.path(), restored.broker_dir.path());
    let restored_broker = start_broker(restored.broker_dir.path(), BootstrapMode::Rejoin).await;
    wait_for_ready(&restored_broker).await;
    restore_deployment_backup(
        &backup,
        &cut_broker_state(&restored_broker.listen_addr().to_string()),
        &restored.parts(),
    )
    .await
    .expect("restore the deployment");

    check!(objects_under(&restored.metrics, UPLOAD_PREFIX).await == records);
    check!(manifest_keys(&restored.metrics).await == manifests);
    check!(imported_sample_count(&restored.metrics).await == samples);
    let uploads = mimir_upload_router(&restored.metrics);
    let same_ulid = send(
        &uploads,
        "POST",
        &upload_uri(FIXTURE_ULID, "start"),
        fixture_upload_meta(FIXTURE_ULID),
    )
    .await;
    let other_ulid = upload_fixture_block(&uploads, OTHER_ULID).await;
    check!(same_ulid == (StatusCode::CONFLICT, b"block already exists\n".to_vec()));
    check!(other_ulid == json!({"result": "complete", "existingBlock": FIXTURE_ULID}));
    check!(manifest_keys(&restored.metrics).await == manifests);
    check!(imported_sample_count(&restored.metrics).await == samples);
}

fn cut_broker_state(bootstrap: &str) -> KafkaBrokerState {
    KafkaBrokerState::new(bootstrap, None, ALL_TOPICS.iter().map(|topic| topic.name))
}

/// Starts a broker on `dir`.
async fn start_broker(dir: &Path, mode: BootstrapMode) -> BrokerHandle {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.bootstrap_mode = mode;
    Broker::start(config).await.expect("broker start")
}

/// Waits until a restarted broker reports, over the Kafka protocol, the end
/// of every partition log it holds on disk.
///
/// A restarted broker answers before it has opened every log and before its
/// high watermark reaches the log end. Until then it reports offset 0, which
/// a restore correctly refuses as a broker of another time.
async fn wait_for_ready(broker: &BrokerHandle) {
    let state = cut_broker_state(&broker.listen_addr().to_string());
    let deadline = Instant::now() + BROKER_DEADLINE;
    loop {
        let reported = state.capture().await.ok();
        let ready = reported.as_ref().is_some_and(|snapshot| {
            ALL_TOPICS.iter().all(|topic| {
                let on_disk = broker.local_log_end_offset(topic.name, 0);
                on_disk.is_some()
                    && snapshot.wal_offsets.iter().any(|offset| {
                        offset.topic == topic.name && Some(offset.next_offset) == on_disk
                    })
            })
        });
        if ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the restarted broker did not become ready: {reported:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy file");
        }
    }
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/// One generation of records for `tenant` on all four WALs.
///
/// Generation 0 goes to the live deployment before the cut, generation 1 to
/// the restored deployment after it. Each generation writes one record per
/// WAL, so the record counts in a query answer say how many generations it
/// holds.
async fn produce_generation(
    producer: &Arc<Producer>,
    bootstrap: &str,
    tenant: &str,
    generation: i64,
) {
    let ts_ms = BASE_MS + generation * 60_000;
    let sample = WalRecord {
        tenant: tenant.into(),
        labels: vec![
            ("__name__".into(), "backup_probe".into()),
            ("job".into(), "checkout".into()),
        ],
        payload: SamplePayload::Float {
            timestamp_ms: ts_ms,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    };
    let exemplar = WalRecord {
        payload: SamplePayload::Exemplars,
        exemplars: vec![WalExemplar {
            labels: vec![("trace_id".into(), hex::encode(TRACE_ID))],
            value: 1.0,
            timestamp_ms: ts_ms,
        }],
        ..sample.clone()
    };
    let metrics = MetricsKafkaSink::new(Arc::clone(producer));
    for record in [sample, exemplar] {
        metrics
            .append(partition_key(tenant, record.series_fingerprint()), record)
            .await
            .expect("metrics WAL append");
    }

    KafkaLogWalSink::connect(bootstrap.to_string(), LOGS_WAL_TOPIC)
        .await
        .expect("logs WAL sink")
        .append(WalLogRecord {
            tenant: tenant.into(),
            labels: labels([("app", "checkout")]),
            timestamp_ns: ts_ms * 1_000_000,
            line: format!("generation {generation} trace_id={}", hex::encode(TRACE_ID)),
            structured_metadata: BTreeMap::from([("trace_id".to_string(), hex::encode(TRACE_ID))]),
            position: None,
        })
        .await
        .expect("logs WAL append");

    let span_id = u64::try_from(generation + 1).expect("span id");
    TracesKafkaSink::new(Arc::clone(producer))
        .append(SpanRecord {
            tenant: tenant.into(),
            span: Span {
                trace_id: TRACE_ID,
                span_id: span_id.to_be_bytes(),
                parent_span_id: None,
                name: format!("checkout generation {generation}"),
                kind: SpanKind::Server,
                start_ns: ts_ms * 1_000_000,
                duration_ns: 1_000_000,
                status: SpanStatus::Ok,
                status_message: String::new(),
                resource_attrs: vec![krabka_traces::KeyValue {
                    key: "service.name".into(),
                    value: krabka_traces::AttrValue::Str("checkout".into()),
                }],
                span_attrs: Vec::new(),
                events: Vec::new(),
                links: Vec::new(),
                instrumentation_scope: String::new(),
                instrumentation_version: String::new(),
            },
        })
        .await
        .expect("traces WAL append");

    ProfilesKafkaSink::new(Arc::clone(producer))
        .append(profile_record(tenant, ts_ms, span_id))
        .await
        .expect("profiles WAL append");
}

fn profile_record(tenant: &str, ts_ms: i64, span_id: u64) -> ProfileRecord {
    ProfileRecord {
        tenant: tenant.into(),
        labels: vec![
            ("__profile_type__".into(), PROFILE_TYPE.into()),
            ("__name__".into(), "process_cpu".into()),
            ("service_name".into(), "checkout".into()),
        ],
        profile_type: PROFILE_TYPE.into(),
        samples: vec![WalSample {
            stacktrace_location_refs: vec![0],
            value: 7,
            timestamp_ns: ts_ms * 1_000_000,
            span_id: Some(span_id),
            trace_id: Some(TRACE_ID.to_vec()),
        }],
        symbols: WalSymbolSet {
            strings: vec![String::new(), FRAME.into()],
            functions: vec![WalFunction {
                name: 1,
                system_name: 1,
                filename: 0,
                start_line: 0,
            }],
            locations: vec![WalLocation {
                address: 0x1000,
                mapping_id: 0,
                lines: vec![(0, 10)],
            }],
            mappings: vec![WalMapping {
                memory_start: 0,
                memory_limit: 0,
                file_offset: 0,
                filename: 0,
                build_id: 0,
                has_functions: true.into(),
                has_filenames: false.into(),
                has_line_numbers: false.into(),
                has_inline_frames: false.into(),
            }],
        },
    }
}

/// The state that is not a block: rules, alerts, deletes.
async fn write_state(live: &Deployment, bootstrap: &str) {
    let ruler = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap)
            .transactional_id(format!("backup-restore-ruler-{}", unique_suffix()))
            .build()
            .await
            .expect("transactional ruler producer"),
    );
    ruler.init_transactions().await.expect("init transactions");
    for tenant in TENANTS {
        // Mimir rule groups persist in the metrics bucket.
        let mimir = mimir_config_router(&live.metrics).await;
        let response = mimir
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/prometheus/config/v1/rules/backup")
                    .header("X-Scope-OrgID", tenant)
                    .header("Content-Type", "application/yaml")
                    .body(Body::from(
                        "name: probes\nrules:\n  - alert: BackupProbe\n    expr: backup_probe > 0\n    for: 1h\n",
                    ))
                    .expect("rules request"),
            )
            .await
            .expect("rules response");
        assert!(response.status() == StatusCode::ACCEPTED);

        // A metric erasure request is an object in the metrics bucket.
        put_erasure_request(
            &live.metrics,
            ERASURE_REQUEST_PREFIX,
            &ErasureRequest::new(
                tenant,
                r#"{__name__="erased_series"}"#,
                vec![vec![LabelMatcher {
                    name: "__name__".into(),
                    op: MatchOp::Eq,
                    value: "erased_series".into(),
                }]],
                0,
                1,
                BASE_MS * 1_000_000,
            ),
        )
        .await
        .expect("erasure request");

        // Ruler state: one evaluated group and one pending alert, on the
        // compacted topic the ruler replays at start. The ruler writes it
        // with a recording-rule sample in one Kafka transaction, so the
        // metrics WAL ends with a commit marker that no block covers.
        let transaction = ruler.begin_transaction().await.expect("ruler transaction");
        let ruler_state = KafkaRulerStateSink::new(Arc::clone(&ruler), METRICS_RULER_STATE_TOPIC);
        ruler_state
            .persist_ruler_group_state(group_state_record(tenant))
            .await
            .expect("ruler group state");
        ruler_state
            .persist_ruler_alert_state(alert_state_record(tenant))
            .await
            .expect("ruler alert state");
        KafkaRecordingRuleWalSink::new(Arc::clone(&ruler), METRICS_WAL_TOPIC)
            .append_recording_rule_record(recording_rule_record(tenant))
            .await
            .expect("recording rule sample");
        transaction
            .commit()
            .await
            .expect("commit the ruler transaction");

        // Loki rules and a log delete request persist under `data-root`.
        let logs_block_builder = logs_router(live, Role::BlockBuilder).await;
        let response = logs_block_builder
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/loki/api/v1/delete?query=%7Bapp%3D%22checkout%22%7D%20%7C%3D%20%22erased%22&start={}&end={}",
                        BASE_MS / 1_000 - 1,
                        BASE_MS / 1_000 + 1,
                    ))
                    .header("X-Scope-OrgID", tenant)
                    .body(Body::empty())
                    .expect("delete request"),
            )
            .await
            .expect("delete response");
        assert!(response.status() == StatusCode::NO_CONTENT);
        let logs_querier = logs_router(live, Role::Querier).await;
        let response = logs_querier
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/loki/api/v1/rules/backup")
                    .header("X-Scope-OrgID", tenant)
                    .header("Content-Type", "application/yaml")
                    .body(Body::from(
                        "name: checkout-errors\nrules:\n  - alert: CheckoutErrors\n    expr: count_over_time({app=\"checkout\"} |= \"error\" [5m]) > 0\n",
                    ))
                    .expect("loki rules request"),
            )
            .await
            .expect("loki rules response");
        assert!(response.status() == StatusCode::ACCEPTED);
    }
}

/// The sample a recording rule writes, in the same transaction as its state.
fn recording_rule_record(tenant: &str) -> WalRecord {
    WalRecord {
        tenant: tenant.into(),
        labels: vec![("__name__".into(), "backup_probe:sum".into())],
        payload: SamplePayload::Float {
            timestamp_ms: BASE_MS,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    }
}

fn group_state_record(tenant: &str) -> RulerGroupStateRecord {
    RulerGroupStateRecord {
        tenant: tenant.into(),
        namespace: "backup".into(),
        group: "probes".into(),
        last_eval_ms: BASE_MS,
    }
}

fn alert_state_record(tenant: &str) -> RulerAlertStateRecord {
    RulerAlertStateRecord {
        tenant: tenant.into(),
        rule_id: ALERT_RULE.into(),
        labels: BTreeMap::from([
            ("alertname".to_string(), "BackupProbe".to_string()),
            ("job".to_string(), "checkout".to_string()),
        ])
        .into(),
        active_since_ms: Some(BASE_MS),
        keep_firing_until_ms: None,
    }
}

/// The Mimir configuration API over `store`, with every persisted tenant
/// configuration loaded, as the ruler loads it at start.
async fn mimir_config_router(store: &Arc<dyn ObjectStore>) -> axum::Router {
    let state = Arc::new(
        krabka_promql::PrometheusApiState::new(
            Arc::new(InMemoryMetricStore::new()),
            krabka_promql::EngineOpts::default(),
        )
        .with_mimir_config_store(Arc::clone(store)),
    );
    state
        .reload_mimir_configs()
        .await
        .expect("reload Mimir configs");
    authenticate_requests(
        krabka_promql::prometheus_router(state),
        &ServerSecurity::default(),
    )
}

// ---------------------------------------------------------------------------
// Block builders
// ---------------------------------------------------------------------------

/// Runs every block builder until it has written and committed the end of
/// its WAL.
async fn build_all_blocks(deployment: &Deployment, bootstrap: &str) {
    build_metrics_blocks(&deployment.metrics, bootstrap).await;
    build_logs_blocks(deployment, bootstrap).await;
    build_traces_blocks(&deployment.traces, bootstrap).await;
    build_profiles_blocks(&deployment.profiles, bootstrap).await;
    wait_for_drained(bootstrap).await;
}

async fn build_metrics_blocks(store: &Arc<dyn ObjectStore>, bootstrap: &str) {
    let metrics = MetricsServiceMetrics::new();
    let mut config = MetricsCompactorConfig::new(bootstrap);
    config.group_id = METRICS_GROUP.into();
    config.client_id = METRICS_GROUP.into();
    config.poll_timeout = millis(200);
    let runtime = config
        .build_runtime(Arc::clone(store), metrics.object_store.clone())
        .expect("metrics compactor runtime");
    let mut consumer = config
        .build_consumer(&metrics.wal_consumer, None, None)
        .await
        .expect("metrics compactor consumer");
    // Stop after three empty polls in a row once something arrived, or after
    // ten empty polls when nothing new is in the WAL.
    let mut seen = 0;
    let mut empty = 0;
    run_compactor_consumer_loop(
        &mut consumer,
        &runtime.block_writer,
        &runtime.index_sink,
        runtime.loop_config,
        |poll| {
            seen += poll.compacted_records;
            empty = if poll.polled_records == 0 {
                empty + 1
            } else {
                0
            };
            (seen > 0 && empty >= 3) || empty >= 10
        },
        &metrics,
    )
    .await
    .expect("metrics compactor");
}

async fn build_logs_blocks(deployment: &Deployment, bootstrap: &str) {
    let consumer = KafkaLogWalConsumer::connect(bootstrap.to_string(), LOGS_GROUP, LOGS_WAL_TOPIC)
        .await
        .expect("logs consumer");
    let config = logs_config(deployment, Role::BlockBuilder, Some(bootstrap));
    // The first poll of a fresh group can return nothing before the broker
    // assigns its partition, so this repeats until the group is drained.
    run_compactor_until_idle(
        &config,
        ServiceDependencies::default().with_wal_consumer(consumer),
        Some(deployment.logs.as_ref()),
    )
    .await
    .expect("logs compactor");
}

async fn build_traces_blocks(store: &Arc<dyn ObjectStore>, bootstrap: &str) {
    let consumer = krabka_client_consumer::Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(TRACES_GROUP)
        .subscribe(vec![TRACES_WAL_TOPIC.to_string()])
        .auto_offset_reset(krabka_client_consumer::AutoOffsetReset::Earliest)
        .isolation_level(krabka_client_consumer::IsolationLevel::ReadCommitted)
        .enable_auto_commit(false)
        .build()
        .await
        .expect("traces consumer");
    let index = TraceIndex::load_latest_snapshot(store, TRACE_INDEX_KEY)
        .await
        .unwrap_or_default();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(krabka_traces::blockbuilder::run(
        BlockBuilderConsumer::new(consumer, &WalConsumerMetrics::unregistered()),
        krabka_blockstore::BlockWriter::new(Arc::clone(store)),
        Arc::new(Mutex::new(index)),
        Arc::clone(store),
        TracesBlockBuilderConfig {
            object_key_prefix: String::new(),
            index_key: TRACE_INDEX_KEY.into(),
            window: millis(200),
            empty_poll_backoff: millis(50),
            promoted_attrs: Vec::new(),
            flush_max_records: 1,
            flush_max_age: millis(1),
            index_snapshot_retain: krabka_blockstore::IndexSnapshotRetain::default(),
        },
        TracesServiceMetrics::new(),
        shutdown.clone(),
    ));
    wait_for_group(bootstrap, TRACES_GROUP, TRACES_WAL_TOPIC).await;
    shutdown.cancel();
    task.await
        .expect("traces block builder task")
        .expect("traces block builder");
}

async fn build_profiles_blocks(store: &Arc<dyn ObjectStore>, bootstrap: &str) {
    let mut config = ProfilesBlockBuilderConfig::new(bootstrap.to_string(), Arc::clone(store));
    config.group_id = PROFILES_GROUP.into();
    config.index_key = PROFILE_INDEX_KEY.into();
    config.flush_records = 1;
    config.flush_max_age = millis(1);
    config.poll_timeout = millis(200);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run_with_config(config, shutdown.clone()));
    wait_for_group(bootstrap, PROFILES_GROUP, PROFILES_WAL_TOPIC).await;
    shutdown.cancel();
    task.await
        .expect("profiles block builder task")
        .expect("profiles block builder");
}

/// Waits until `group` has no record of `topic` after its committed offset.
///
/// A trailing transaction marker takes an offset and is not a record, so the
/// committed offset can stay one below the end.
async fn wait_for_group(bootstrap: &str, group: &str, topic: &str) {
    let broker = cut_broker_state(bootstrap);
    let deadline = Instant::now() + BROKER_DEADLINE;
    loop {
        let snapshot = broker.capture().await.expect("read the broker");
        let end = snapshot
            .wal_offsets
            .iter()
            .find(|offset| offset.topic == topic)
            .map_or(0, |offset| offset.next_offset);
        let committed = snapshot
            .group_offsets
            .iter()
            .find(|offset| offset.group == group && offset.topic == topic)
            .map_or(0, |offset| offset.next_offset);
        let pending = broker
            .records_between(topic, 0, committed, end)
            .await
            .expect("count pending records");
        if pending == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{group} has {pending} records of {topic} after offset {committed}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_drained(bootstrap: &str) {
    for group in deployment_drained_groups() {
        wait_for_group(bootstrap, &group.group, &group.topic).await;
    }
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// Every answer that must survive the restore, keyed by a stable name.
async fn query_everything(deployment: &Deployment, bootstrap: &str) -> BTreeMap<String, Value> {
    let mut answers = BTreeMap::new();
    let metrics = metrics_router(&deployment.metrics).await;
    let logs = logs_router(deployment, Role::Querier).await;
    let logs_block_builder = logs_router(deployment, Role::BlockBuilder).await;
    let traces = traces_engine(&deployment.traces).await;
    let profiles = profiles_router(&deployment.profiles).await;
    let mimir = mimir_config_router(&deployment.metrics).await;
    let ruler_state = replayed_ruler_state(bootstrap).await;
    for tenant in TENANTS {
        let end_s = BASE_MS / 1_000 + 3_600;
        answers.insert(
            format!("{tenant}/metrics/query_range"),
            get_json(
                &metrics,
                tenant,
                &format!(
                    "/api/v1/query_range?query=backup_probe&start={}&end={end_s}&step=60",
                    BASE_MS / 1_000
                ),
            )
            .await,
        );
        for (name, query) in [
            (
                "compound",
                "(sum(sum_over_time(backup_probe[1h1ms])) + 2) * 3",
            ),
            ("absent", "sum(backup_probe{job=\"absent\"})"),
        ] {
            let query = url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>();
            answers.insert(
                format!("{tenant}/metrics/{name}"),
                get_json(
                    &metrics,
                    tenant,
                    &format!("/api/v1/query?query={query}&time={end_s}"),
                )
                .await,
            );
        }
        let query = url::form_urlencoded::byte_serialize(
            b"sum(count_over_time({app=\"checkout\"} |= \"generation\" [1h1ms]))",
        )
        .collect::<String>();
        answers.insert(
            format!("{tenant}/logs/compound"),
            without_stats(
                get_json(
                    &logs,
                    tenant,
                    &format!("/loki/api/v1/query?query={query}&time={end_s}"),
                )
                .await,
            ),
        );
        let trace_metrics = traces
            .query_range(
                tenant,
                "{resource.service.name = \"checkout\"} | count_over_time()",
                BASE_MS * 1_000_000,
                (BASE_MS + 3_600_000) * 1_000_000,
                3_600_000_000_000,
            )
            .await
            .expect("TraceQL count ledger");
        answers.insert(
            format!("{tenant}/traces/metrics"),
            serde_json::to_value(trace_metrics.series).expect("metrics ledger JSON"),
        );
        let search = traces
            .search(
                tenant,
                "{resource.service.name = \"checkout\" && duration > 500us}",
                (BASE_MS - 1) * 1_000_000,
                (BASE_MS + 3_600_000) * 1_000_000,
                100,
            )
            .await
            .expect("TraceQL compound ledger");
        let mut selected = search
            .traces
            .iter()
            .flat_map(|trace| {
                trace.span_sets.iter().flat_map(move |set| {
                    set.spans
                        .iter()
                        .map(move |span| (hex::encode(trace.trace_id), hex::encode(span.span_id)))
                })
            })
            .collect::<Vec<_>>();
        selected.sort();
        answers.insert(format!("{tenant}/traces/compound"), json!(selected));
        answers.insert(
            format!("{tenant}/metrics/recording_rule"),
            get_json(
                &metrics,
                tenant,
                &format!(
                    "/api/v1/query?query=backup_probe:sum&time={}",
                    BASE_MS / 1_000
                ),
            )
            .await,
        );
        answers.insert(
            format!("{tenant}/metrics/query_exemplars"),
            get_json(
                &metrics,
                tenant,
                &format!(
                    "/api/v1/query_exemplars?query=backup_probe&start={}&end={end_s}",
                    BASE_MS / 1_000 - 60
                ),
            )
            .await,
        );
        answers.insert(
            format!("{tenant}/metrics/rules"),
            get_text(&mimir, tenant, "/prometheus/config/v1/rules").await,
        );
        answers.insert(
            format!("{tenant}/metrics/erasure_requests"),
            erasure_requests(&deployment.metrics, tenant).await,
        );
        answers.insert(
            format!("{tenant}/metrics/ruler_state"),
            ruler_state.get(tenant).cloned().unwrap_or(Value::Null),
        );
        answers.insert(
            format!("{tenant}/logs/query_range"),
            without_stats(get_json(
                &logs,
                tenant,
                &format!(
                    "/loki/api/v1/query_range?query=%7Bapp%3D%22checkout%22%7D&start={}&end={}&direction=forward&limit=100",
                    (BASE_MS - 1) * 1_000_000,
                    (BASE_MS + 3_600_000) * 1_000_000,
                ),
            )
            .await),
        );
        answers.insert(
            format!("{tenant}/logs/rules"),
            get_text(&logs, tenant, "/loki/api/v1/rules/backup").await,
        );
        answers.insert(
            format!("{tenant}/logs/delete_requests"),
            get_json(&logs_block_builder, tenant, "/loki/api/v1/delete").await,
        );
        let trace = traces
            .trace_by_id(tenant, &TRACE_ID)
            .await
            .expect("trace by id")
            .map(|trace| {
                let mut spans = trace
                    .spans
                    .iter()
                    .map(|span| (hex::encode(span.span_id), span.name.clone()))
                    .collect::<Vec<_>>();
                spans.sort();
                json!({"root": trace.root_trace_name, "spans": spans})
            });
        answers.insert(format!("{tenant}/traces/trace_by_id"), json!(trace));
        for (name, selector) in [
            (
                "filtered",
                "{service_name=~\"check.*\",service_name!=\"absent\"}",
            ),
            ("absent", "{service_name=\"absent\"}"),
        ] {
            let query = url::form_urlencoded::byte_serialize(
                format!("{PROFILE_TYPE}{selector}").as_bytes(),
            )
            .collect::<String>();
            answers.insert(
                format!("{tenant}/profiles/{name}"),
                get_json(
                    &profiles,
                    tenant,
                    &format!("/pyroscope/render?query={query}&from=0&until={}", i64::MAX),
                )
                .await,
            );
        }
        answers.insert(
            format!("{tenant}/profiles/render"),
            get_json(
                &profiles,
                tenant,
                &format!(
                    "/pyroscope/render?query={}&from=0&until={}",
                    url::form_urlencoded::byte_serialize(
                        format!("{PROFILE_TYPE}{{service_name=\"checkout\"}}").as_bytes()
                    )
                    .collect::<String>(),
                    i64::MAX
                ),
            )
            .await,
        );
    }
    answers
}

/// Checks the answers against what `generations` generations of writes
/// produce, so a restore that answers wrongly in the same way as the live
/// deployment still fails.
fn check_expected_answers(answers: &BTreeMap<String, Value>, generations: usize) {
    let trace_id = hex::encode(TRACE_ID);
    for tenant in TENANTS {
        let samples =
            &answers[&format!("{tenant}/metrics/query_range")]["data"]["result"][0]["values"];
        // One sample per generation, one minute apart. Each step within the
        // five-minute lookback of the newest sample has a point.
        check!(
            samples.as_array().map(Vec::len) == Some(4 + generations),
            "{tenant}: one point per step within the lookback"
        );
        check!(
            answers[&format!("{tenant}/metrics/recording_rule")]["data"]["result"][0]["value"][1]
                == json!("1"),
            "{tenant}: the transactional recording-rule sample"
        );
        let exemplars = answers[&format!("{tenant}/metrics/query_exemplars")]["data"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|group| group["exemplars"].as_array().cloned().unwrap_or_default())
            .map(|exemplar| exemplar["labels"]["trace_id"].clone())
            .collect::<Vec<_>>();
        check!(
            exemplars == vec![json!(trace_id); generations],
            "{tenant}: every exemplar names the trace"
        );
        check!(
            answers[&format!("{tenant}/metrics/rules")]
                .as_str()
                .is_some_and(|rules| rules.contains("alert: BackupProbe")),
            "{tenant}: Mimir rule group"
        );
        check!(
            answers[&format!("{tenant}/metrics/erasure_requests")]
                .as_array()
                .map(Vec::len)
                == Some(1),
            "{tenant}: metric erasure request"
        );
        check!(
            answers[&format!("{tenant}/metrics/ruler_state")]
                == json!([
                    RulerStateWalRecord::Group(group_state_record(tenant)),
                    RulerStateWalRecord::Alert(alert_state_record(tenant)),
                ]),
            "{tenant}: evaluated group and pending alert"
        );
        let lines = answers[&format!("{tenant}/logs/query_range")]["data"]["result"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|stream| stream["values"].as_array().cloned().unwrap_or_default())
            .map(|value| value[1].clone())
            .collect::<Vec<_>>();
        check!(
            lines
                == (0..generations)
                    .map(|generation| json!(format!("generation {generation} trace_id={trace_id}")))
                    .collect::<Vec<_>>(),
            "{tenant}: log lines carry the trace id"
        );
        check!(
            answers[&format!("{tenant}/logs/rules")]
                .as_str()
                .is_some_and(|rules| rules.contains("alert: CheckoutErrors")),
            "{tenant}: Loki rule group"
        );
        check!(
            answers[&format!("{tenant}/logs/delete_requests")]
                .as_array()
                .map(Vec::len)
                == Some(1),
            "{tenant}: log delete request"
        );
        check!(
            answers[&format!("{tenant}/traces/trace_by_id")]["spans"]
                .as_array()
                .map(Vec::len)
                == Some(generations),
            "{tenant}: one span per generation"
        );
        check!(
            answers[&format!("{tenant}/metrics/compound")]["data"]["result"][0]["value"][1]
                == json!(((generations + 2) * 3).to_string()),
            "{tenant}: composed PromQL value"
        );
        check!(
            answers[&format!("{tenant}/metrics/absent")]["data"]["result"] == json!([]),
            "{tenant}: absent selector stays empty"
        );
        check!(
            answers[&format!("{tenant}/logs/compound")]["data"]["result"][0]["value"][1]
                == json!(generations.to_string()),
            "{tenant}: composed LogQL exact multiplicity"
        );
        check!(
            answers[&format!("{tenant}/traces/metrics")]
                == json!([{
                    "labels": [], "points": [[BASE_MS * 1_000_000, f64::from(u32::try_from(generations).unwrap())], [(BASE_MS + 3_600_000) * 1_000_000, 0.0]], "exemplars": []
                }]),
            "{tenant}: exact TraceQL points across restore"
        );
        check!(
            answers[&format!("{tenant}/traces/compound")]
                == json!(
                    (1..=generations)
                        .map(|span| (
                            trace_id.clone(),
                            hex::encode(u64::try_from(span).unwrap().to_be_bytes())
                        ))
                        .collect::<Vec<_>>()
                ),
            "{tenant}: exact selected trace/span IDs"
        );
        check!(
            answers[&format!("{tenant}/profiles/filtered")]["flamebearer"]["numTicks"]
                == json!(7 * generations),
            "{tenant}: profile regex and negative selector compose"
        );
        check!(
            answers[&format!("{tenant}/profiles/absent")]["flamebearer"]["numTicks"] == json!(0),
            "{tenant}: profile absence stays empty"
        );
        let render = &answers[&format!("{tenant}/profiles/render")];
        check!(
            render["flamebearer"]["names"]
                .as_array()
                .is_some_and(|names| names.contains(&json!(FRAME))),
            "{tenant}: profile symbols resolve"
        );
        check!(
            render["flamebearer"]["numTicks"] == json!(7 * generations),
            "{tenant}: one profile sample per generation"
        );
    }
}

async fn metrics_router(store: &Arc<dyn ObjectStore>) -> axum::Router {
    authenticate_requests(
        blockstore_prometheus_router(
            Arc::clone(store),
            url::Url::parse("memory:///").expect("url"),
            "metrics",
        )
        .await
        .expect("metrics router"),
        &ServerSecurity::default(),
    )
}

fn logs_config(deployment: &Deployment, target: Role, bootstrap: Option<&str>) -> ServiceConfig {
    ServiceConfig {
        target,
        listen_addr: "127.0.0.1:0".parse().expect("listen addr"),
        object_store_url: Some(format!("file://{}", deployment.logs_dir.path().display())),
        wal_bootstrap_server: bootstrap.map(str::to_string),
        wal_topic: LOGS_WAL_TOPIC.into(),
        wal_group_id: LOGS_GROUP.into(),
        data_root: deployment.logs_state_dir.path().to_path_buf(),
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: None,
        index_prefix: Some(LOGS_INDEX_PREFIX.into()),
        ..ServiceConfig::default()
    }
}

/// A logs router with no WAL connection, so the querier answers from the
/// object store and `data-root` alone, as a restored querier does before it
/// reaches the broker.
async fn logs_router(deployment: &Deployment, target: Role) -> axum::Router {
    build_service_router(
        &logs_config(deployment, target, None),
        ServiceDependencies::default(),
        None,
    )
    .await
    .expect("logs router")
}

async fn traces_engine(store: &Arc<dyn ObjectStore>) -> TraceqlEngine<KrabkaSpanStore> {
    let index = TraceIndex::load_latest_snapshot(store, TRACE_INDEX_KEY)
        .await
        .expect("trace index");
    TraceqlEngine::new(
        Arc::new(KrabkaSpanStore::new(
            Arc::new(BlockStore::new(
                Arc::clone(store),
                url::Url::parse("memory:///").expect("url"),
            )),
            Arc::new(ArcSwap::from_pointee(index)),
            None,
        )),
        TraceqlOpts::default(),
    )
}

async fn profiles_router(store: &Arc<dyn ObjectStore>) -> axum::Router {
    let index = ProfileIndex::load_latest_snapshot(store, PROFILE_INDEX_KEY)
        .await
        .expect("profile index");
    let querier = Arc::new(QuerierState::new_with_limits(
        Arc::new(ColdProfileStore::new(Arc::clone(store), Arc::new(index))),
        Limits {
            max_query_length: Time::ZERO,
            ..Default::default()
        },
    ));
    authenticate_requests(query::router(querier), &ServerSecurity::default())
}

/// The ruler state that a starting ruler replays from the compacted state
/// topic, decoded with the ruler's own codec, by tenant.
async fn replayed_ruler_state(bootstrap: &str) -> BTreeMap<String, Value> {
    let mut consumer = krabka_client_consumer::Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(format!("backup-restore-ruler-state-{}", unique_suffix()))
        .subscribe(vec![METRICS_RULER_STATE_TOPIC.to_string()])
        .auto_offset_reset(krabka_client_consumer::AutoOffsetReset::Earliest)
        .isolation_level(krabka_client_consumer::IsolationLevel::ReadCommitted)
        .enable_auto_commit(false)
        .build()
        .await
        .expect("ruler state consumer");
    let deadline = Instant::now() + BROKER_DEADLINE;
    let mut records = Vec::new();
    while records.len() < 2 * TENANTS.len() {
        for record in consumer.poll(millis(200)).await.expect("poll ruler state") {
            let value = record.value.expect("ruler state value");
            records.push(RulerStateWalRecord::decode(&value).expect("ruler state record"));
        }
        assert!(Instant::now() < deadline, "ruler state did not replay");
    }
    let mut by_tenant = BTreeMap::<String, Vec<Value>>::new();
    for record in records {
        let tenant = match &record {
            RulerStateWalRecord::Group(group) => group.tenant.clone(),
            RulerStateWalRecord::Alert(alert) => alert.tenant.clone(),
        };
        by_tenant
            .entry(tenant)
            .or_default()
            .push(serde_json::to_value(record).expect("json"));
    }
    by_tenant
        .into_iter()
        .map(|(tenant, records)| (tenant, Value::Array(records)))
        .collect()
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

async fn erasure_requests(store: &Arc<dyn ObjectStore>, tenant: &str) -> Value {
    let requests = krabka_blockstore::list_erasure_requests(store, ERASURE_REQUEST_PREFIX)
        .await
        .expect("erasure requests")
        .into_iter()
        .filter(|request| request.tenant == tenant)
        .collect::<Vec<_>>();
    serde_json::to_value(requests).expect("json")
}

/// A Loki answer without `data.stats`, which holds query timings rather than
/// stored data.
fn without_stats(mut answer: Value) -> Value {
    if let Some(data) = answer.get_mut("data").and_then(Value::as_object_mut) {
        data.remove("stats");
    }
    answer
}

async fn get_body(router: &axum::Router, tenant: &str, uri: &str) -> Vec<u8> {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", tenant)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
        .to_vec();
    assert!(
        status == StatusCode::OK,
        "{uri}: {status} {}",
        String::from_utf8_lossy(&body)
    );
    body
}

async fn get_json(router: &axum::Router, tenant: &str, uri: &str) -> Value {
    serde_json::from_slice(&get_body(router, tenant, uri).await).expect("json body")
}

async fn get_text(router: &axum::Router, tenant: &str, uri: &str) -> Value {
    Value::String(String::from_utf8(get_body(router, tenant, uri).await).expect("utf-8 body"))
}

// ---------------------------------------------------------------------------
// Prometheus TSDB import
// ---------------------------------------------------------------------------

/// The Mimir block-upload routes over the metrics bucket, as a metrics
/// querier serves them.
fn mimir_upload_router(store: &Arc<dyn ObjectStore>) -> axum::Router {
    let head = WalHead::new();
    let query_store = Arc::new(RefreshingMetricBlockStore::new(
        Arc::clone(store),
        url::Url::parse("memory:///").expect("url"),
        "metrics",
        head.clone(),
    ));
    authenticate_requests(
        mimir_tenant_admin_router(MimirTenantAdminState::new(
            Arc::clone(store),
            query_store,
            head,
        )),
        &ServerSecurity::default(),
    )
}

fn upload_uri(ulid: &str, step: &str) -> String {
    format!("/api/v1/upload/block/{ulid}/{step}")
}

/// The files of the checked-in Prometheus block, by upload path.
fn fixture_upload_files() -> [(&'static str, Vec<u8>); 3] {
    let files = fixture_files();
    [
        ("index", files.index),
        ("chunks/000001", files.chunks),
        ("tombstones", files.tombstones),
    ]
}

/// The upload `meta.json` of the checked-in block, as `mimirtool backfill`
/// sends it to `start`.
fn fixture_upload_meta(ulid: &str) -> Vec<u8> {
    let files = std::iter::once(json!({"rel_path": "meta.json"}))
        .chain(
            fixture_upload_files()
                .iter()
                .map(|(path, bytes)| json!({"rel_path": path, "size_bytes": bytes.len()})),
        )
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "ulid": ulid,
        "minTime": FIXTURE_MIN_TIME,
        "maxTime": FIXTURE_MAX_TIME,
        "version": 1,
        "thanos": {"files": files},
    }))
    .expect("encode meta.json")
}

async fn send(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Vec<u8>,
) -> (StatusCode, Vec<u8>) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("X-Scope-OrgID", TENANTS[0])
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body")
        .to_vec();
    (status, body)
}

/// Uploads the checked-in block under `ulid` with the four requests of
/// `mimirtool backfill`, and returns the `check` answer.
async fn upload_fixture_block(router: &axum::Router, ulid: &str) -> Value {
    let started = send(
        router,
        "POST",
        &upload_uri(ulid, "start"),
        fixture_upload_meta(ulid),
    )
    .await;
    assert!(started.0 == StatusCode::OK, "start: {started:?}");
    for (path, bytes) in fixture_upload_files() {
        let uri = format!(
            "{}?path={}",
            upload_uri(ulid, "files"),
            path.replace('/', "%2F")
        );
        let uploaded = send(router, "POST", &uri, bytes).await;
        assert!(uploaded.0 == StatusCode::OK, "file {path}: {uploaded:?}");
    }
    let finished = send(router, "POST", &upload_uri(ulid, "finish"), Vec::new()).await;
    assert!(finished.0 == StatusCode::OK, "finish: {finished:?}");
    let (status, body) = send(router, "GET", &upload_uri(ulid, "check"), Vec::new()).await;
    assert!(status == StatusCode::OK, "check: {status}");
    serde_json::from_slice(&body).expect("check answer")
}

/// The objects that the upload and the import of the checked-in block leave
/// under the upload prefix: the upload files and state, the binding of the
/// ULID, and the import record of the content hash.
fn expected_upload_keys() -> BTreeSet<String> {
    let files = fixture_files();
    let sha256 = tsdb_block_sha256(TsdbBlockFiles {
        index: &files.index,
        chunk_segments: &[files.chunks.as_slice()],
        tombstones: Some(&files.tombstones),
    });
    let tenant = format!("{UPLOAD_PREFIX}/{}", TENANTS[0]);
    let upload = format!("{tenant}/{FIXTURE_ULID}");
    [
        format!("{upload}/files/chunks/000001"),
        format!("{upload}/files/index"),
        format!("{upload}/files/tombstones"),
        format!("{upload}/import.json"),
        format!("{upload}/state.json"),
        format!("{upload}/uploading-meta.json"),
        format!("{tenant}/by-sha256/{sha256}.json"),
    ]
    .into_iter()
    .collect()
}

/// Every object under `prefix`, with its bytes.
async fn objects_under(store: &Arc<dyn ObjectStore>, prefix: &str) -> BTreeMap<String, Vec<u8>> {
    let mut objects = BTreeMap::new();
    for path in list_paths(store).await {
        if path.starts_with(&format!("{prefix}/")) {
            let bytes = store
                .get(&object_store::path::Path::from(path.as_str()))
                .await
                .expect("get")
                .bytes()
                .await
                .expect("object bytes");
            objects.insert(path, bytes.to_vec());
        }
    }
    objects
}

async fn manifest_keys(store: &Arc<dyn ObjectStore>) -> BTreeSet<String> {
    list_paths(store)
        .await
        .into_iter()
        .filter(|path| {
            Path::new(path)
                .extension()
                .is_some_and(|extension| extension == "index")
        })
        .collect()
}

/// The `[time, value]` pair of the count of every imported sample, as the
/// metrics query path reads it from the manifests in `store`.
async fn imported_sample_count(store: &Arc<dyn ObjectStore>) -> Value {
    let query = url::form_urlencoded::byte_serialize(
        br#"sum(count_over_time({__name__=~"imported_.+"}[3h]))"#,
    )
    .collect::<String>();
    let answer = get_json(
        &metrics_router(store).await,
        TENANTS[0],
        &format!(
            "/api/v1/query?query={query}&time={}",
            FIXTURE_MAX_TIME / 1_000
        ),
    )
    .await;
    check!(answer["status"] == "success", "{answer}");
    answer["data"]["result"][0]["value"].clone()
}

// ---------------------------------------------------------------------------
// WAL coverage
// ---------------------------------------------------------------------------

/// The WAL offsets that each block series of each signal covers.
///
/// A block key names the offset range of the poll batch it came from. One
/// batch can write a block for each tenant and block kind, so a block series
/// is one signal, tenant, and kind. Inside a series, an offset that appears
/// twice is a record that two blocks published.
async fn wal_coverage(deployment: &Deployment) -> BTreeMap<String, BTreeMap<String, Vec<i64>>> {
    let mut coverage = BTreeMap::<String, BTreeMap<String, Vec<i64>>>::new();
    let mut add = |signal: &str, series: String, range: std::ops::RangeInclusive<i64>| {
        coverage
            .entry(signal.into())
            .or_default()
            .entry(series)
            .or_default()
            .extend(range);
    };
    for manifest in list_compaction_manifests(&deployment.metrics)
        .await
        .expect("metrics manifests")
    {
        add(
            "metrics",
            format!("{}/{:?}", manifest.tenant, manifest.kind),
            manifest.first_offset..=manifest.last_offset,
        );
    }
    for path in list_paths(&deployment.logs).await {
        if path.ends_with(".parquet")
            && let Some(range) = offset_range_in(&path, "/offsets=", '-')
        {
            let tenant = path
                .split('/')
                .find_map(|segment| segment.strip_prefix("tenant="))
                .expect("tenant segment");
            add("logs", tenant.into(), range);
        }
    }
    let traces = TraceIndex::load_latest_snapshot(&deployment.traces, TRACE_INDEX_KEY)
        .await
        .expect("trace index");
    for tenant in traces.tenants() {
        for block in traces.trace_blocks(&tenant) {
            add(
                "traces",
                tenant.clone(),
                block_key_offsets(&block.object_key),
            );
        }
    }
    let profiles = ProfileIndex::load_latest_snapshot(&deployment.profiles, PROFILE_INDEX_KEY)
        .await
        .expect("profile index");
    for block in profiles.all_blocks() {
        add(
            "profiles",
            block.tenant.clone(),
            block_key_offsets(&block.object_key),
        );
    }
    for series in coverage.values_mut().flat_map(BTreeMap::values_mut) {
        series.sort_unstable();
    }
    coverage
}

/// Whether no series repeats an offset, the series cover every record, and
/// no series covers an offset at or past `end`.
///
/// A block key names the offset range of its batch, so a range can include a
/// transaction marker inside it. A marker after the last record of a batch
/// is in no range.
fn covers_exactly_once(
    series: &BTreeMap<String, Vec<i64>>,
    records: &std::collections::BTreeSet<i64>,
    end: i64,
) -> bool {
    let no_repeats = series
        .values()
        .all(|offsets| offsets.windows(2).all(|pair| pair[0] < pair[1]));
    let covered = series
        .values()
        .flatten()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    no_repeats && covered.is_superset(records) && covered.iter().all(|offset| *offset < end)
}

/// The offset of every record a `read_committed` consumer reads from the
/// start of partition 0 of `topic` up to `end`.
async fn record_offsets(bootstrap: &str, topic: &str, end: i64) -> std::collections::BTreeSet<i64> {
    let mut consumer = krabka_client_consumer::Consumer::builder()
        .bootstrap(bootstrap)
        .isolation_level(krabka_client_consumer::IsolationLevel::ReadCommitted)
        .auto_offset_reset(krabka_client_consumer::AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await
        .expect("record reader");
    consumer
        .assign(&[(topic.to_string(), 0)])
        .await
        .expect("assign");
    let deadline = Instant::now() + BROKER_DEADLINE;
    let mut offsets = std::collections::BTreeSet::new();
    while consumer.position(topic, 0).await.expect("position") < end {
        for record in consumer.poll(millis(200)).await.expect("poll") {
            offsets.insert(record.offset);
        }
        assert!(Instant::now() < deadline, "{topic} did not read to {end}");
    }
    offsets
}

/// The offsets in a `{tenant}/{partition}/{first}-{last}-...parquet` key.
fn block_key_offsets(key: &str) -> std::ops::RangeInclusive<i64> {
    let name = key.rsplit('/').next().expect("file name");
    let mut fields = name.split('-');
    let first = fields.next().and_then(|field| field.parse().ok());
    let last = fields.next().and_then(|field| field.parse().ok());
    match (first, last) {
        (Some(first), Some(last)) => first..=last,
        _ => panic!("block key `{key}` names no offset range"),
    }
}

fn offset_range_in(
    path: &str,
    marker: &str,
    separator: char,
) -> Option<std::ops::RangeInclusive<i64>> {
    let rest = path.split(marker).nth(1)?;
    let range = rest.split('/').next()?;
    let (first, last) = range.split_once(separator)?;
    Some(first.parse().ok()?..=last.parse().ok()?)
}

async fn list_paths(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let mut paths = futures::TryStreamExt::try_collect::<Vec<_>>(store.list(None))
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

// ---------------------------------------------------------------------------
// Evidence
// ---------------------------------------------------------------------------

/// The recovery evidence bundle: JSON records and a SHA-256 manifest.
struct Evidence {
    dir: Option<PathBuf>,
    files: std::sync::Mutex<BTreeMap<String, String>>,
}

impl Evidence {
    fn from_env() -> Self {
        // Bazel zips its undeclared outputs into the test's `test.outputs`.
        let dir = std::env::var_os("KRABKA_RECOVERY_EVIDENCE_DIR")
            .or_else(|| std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR"))
            .map(PathBuf::from);
        if let Some(dir) = &dir {
            std::fs::create_dir_all(dir).expect("evidence dir");
        }
        Self {
            dir,
            files: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    fn write<T: Serialize>(&self, name: &str, value: &T) {
        let Some(dir) = &self.dir else {
            return;
        };
        let mut bytes = serde_json::to_vec_pretty(value).expect("evidence json");
        bytes.push(b'\n');
        std::fs::write(dir.join(name), &bytes).expect("write evidence");
        self.files
            .lock()
            .expect("evidence")
            .insert(name.into(), hex::encode(Sha256::digest(&bytes)));
    }

    fn seal(&self) {
        let Some(dir) = &self.dir else {
            return;
        };
        let mut manifest = String::new();
        for (name, sha256) in self.files.lock().expect("evidence").iter() {
            writeln!(manifest, "{sha256}  {name}").expect("write to a string");
        }
        std::fs::write(dir.join("SHA256SUMS"), manifest).expect("write evidence manifest");
    }
}

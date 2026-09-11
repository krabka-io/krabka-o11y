//! The audit layer in one process: its flags, its event constructors, its
//! handle, and its writer over an in-memory sink and a mock clock.
//!
//! The Kafka sink against a real broker is in `tests/audit.rs`.

use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use assert2::{assert, check};
use async_trait::async_trait;
use axum::body::Bytes;
use clap::{Parser, error::ErrorKind};
use krabka_audit::{
    AuditEventClass, AuditLog, AuditStats, ChainState, Checkpoint, GENESIS_HEAD, MemorySink, Seq,
    chain::from_hex32, chain_hash,
};
use krabka_client_producer::{Header, ProducerRecord};
use krabka_ids::PartitionIndex;
use krabka_units::prelude::{TimeExt as _, mebibytes, minutes};
use qubit_clock::{DateTime, MockTime, Utc};
use tokio_util::sync::CancellationToken;

use super::*;

/// Every environment variable the audit flags read.
const ENVIRONMENT: [&str; 9] = [
    "KRABKA_AUDIT_TOPIC",
    "KRABKA_AUDIT_BOOTSTRAP",
    "KRABKA_AUDIT_PARTITION",
    "KRABKA_AUDIT_SPOOL_DIR",
    "KRABKA_AUDIT_SPOOL_MAX",
    "KRABKA_AUDIT_QUEUE_CAPACITY",
    "KRABKA_AUDIT_CHECKPOINT_EVERY",
    "KRABKA_AUDIT_SIGNING_KEY_PATH",
    "KRABKA_AUDIT_SIGNING_KEY_ID",
];

/// The epoch-millisecond time the mock clock starts at.
const START_MS: i64 = 1_700_000_000_000;

/// A service binary's command line, reduced to the flattened audit flags.
#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    audit: AuditArgs,
}

/// Parses `argv` with no audit variable in the environment.
fn parse(argv: &[&str]) -> Result<AuditArgs, clap::Error> {
    temp_env::with_vars_unset(ENVIRONMENT, || {
        Cli::try_parse_from(std::iter::once("krabka-audit-test").chain(argv.iter().copied()))
            .map(|cli| cli.audit)
    })
}

fn default_args() -> AuditArgs {
    AuditArgs {
        topic: None,
        bootstrap: None,
        partition: PartitionIndex(0),
        spool_dir: None,
        spool_max: DEFAULT_AUDIT_SPOOL_MAX,
        queue_capacity: DEFAULT_AUDIT_QUEUE_CAPACITY,
        checkpoint_every: DEFAULT_AUDIT_CHECKPOINT_EVERY,
        signing_key_path: None,
        signing_key_id: None,
    }
}

fn enabled_args() -> AuditArgs {
    AuditArgs {
        topic: Some("krabka-audit".to_owned()),
        ..default_args()
    }
}

fn product() -> ProductInfo {
    krabka_product("krabka-audit-test", "0.0.0")
}

fn mock_time() -> MockTime {
    MockTime::at(DateTime::<Utc>::from_timestamp_millis(START_MS).expect("the start time is valid"))
}

fn clocks(time: &MockTime) -> AuditClocks {
    AuditClocks {
        clock: Arc::new(time.clock()),
        sleeper: Arc::new(time.sleeper()),
    }
}

fn alice() -> AuditPrincipal {
    principal("alice", MECHANISM_BASIC)
}

fn client() -> AuditEndpoint {
    source_endpoint("10.0.0.7:51000".parse().expect("the address is valid"))
}

/// One event of each kind a service emits.
fn three_events() -> Vec<AuditEvent> {
    vec![
        admin_operation(
            alice(),
            client(),
            OPERATION_DELETE_REQUEST_CREATE,
            vec![
                resource(RESOURCE_TENANT, "tenant-a"),
                resource(RESOURCE_DELETE_REQUEST, "request-1"),
            ],
            AuditOutcome::Success,
            EpochMs(START_MS),
        ),
        authorization_denied(
            alice(),
            client(),
            RESOURCE_TENANT,
            "tenant-b",
            OPERATION_TENANT_READ,
            EpochMs(START_MS + 1),
        ),
        authentication(
            AuditOutcome::Failure,
            MECHANISM_BEARER,
            unauthenticated_principal(),
            client(),
            Some("unknown token".to_owned()),
            EpochMs(START_MS + 2),
        ),
    ]
}

/// The records the writer must hand the sink for `events`, chained on from
/// `chain`.
fn expected_records(events: &[AuditEvent], mut chain: ChainState) -> Vec<AuditRecord> {
    events
        .iter()
        .map(|event| {
            let mut record = AuditRecord::from_event(event, &product());
            let (seq, prev_head) = chain.extend(&record.value);
            record.push_chain_headers(seq, &prev_head);
            record
        })
        .collect()
}

fn header(record: &AuditRecord, key: &str) -> Option<String> {
    record
        .headers
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
}

/// Walks the chain the way the verifier does: each record's `seq` is the
/// next number, and its `prev_hash` is the head after the record before it.
fn check_chain(records: &[AuditRecord]) {
    let mut head = GENESIS_HEAD;
    for (seq, record) in (0_u64..).zip(records) {
        let prev_hash = header(record, "prev_hash").and_then(|hex| from_hex32(&hex));
        check!(
            (header(record, "seq"), prev_hash) == (Some(seq.to_string()), Some(head)),
            "record {seq}"
        );
        head = chain_hash(&head, seq, &record.value);
    }
}

/// Yields to the writer task until `condition` holds.
async fn await_until(what: &str, condition: impl Fn() -> bool) {
    for _ in 0..100_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held: {what}");
}

/// A sink that refuses every write while `fail` is set.
#[derive(Debug, Default)]
struct FailableSink {
    fail: AtomicBool,
    inner: MemorySink,
}

#[async_trait]
impl AuditSink for FailableSink {
    async fn write(&self, record: AuditRecord) -> Result<(), AuditError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(AuditError::Sink("the topic is down".to_owned()));
        }
        self.inner.write(record).await
    }
}

/// A sink whose write never completes, like a broker that never acks.
#[derive(Debug)]
struct StuckSink;

#[async_trait]
impl AuditSink for StuckSink {
    async fn write(&self, _record: AuditRecord) -> Result<(), AuditError> {
        std::future::pending().await
    }
}

#[test]
fn audit_is_off_when_no_audit_flag_is_set() {
    let args = parse(&[]).expect("no flags is a valid command line");

    check!(args == default_args());
    check!(!args.is_enabled());
}

#[test]
fn every_audit_flag_parses_into_its_field() {
    let expected = AuditArgs {
        topic: Some("krabka-audit".to_owned()),
        bootstrap: Some("broker:9092".to_owned()),
        partition: PartitionIndex(3),
        spool_dir: Some(PathBuf::from("/var/lib/krabka/audit-spool")),
        spool_max: mebibytes(512),
        queue_capacity: NonZeroUsize::new(16).expect("16 is not zero"),
        checkpoint_every: minutes(5),
        signing_key_path: Some(PathBuf::from("/etc/krabka/audit.pk8")),
        signing_key_id: Some("audit-2026".to_owned()),
    };

    let from_flags = parse(&[
        "--audit-topic",
        "krabka-audit",
        "--audit-bootstrap",
        "broker:9092",
        "--audit-partition",
        "3",
        "--audit-spool-dir",
        "/var/lib/krabka/audit-spool",
        "--audit-spool-max",
        "512MiB",
        "--audit-queue-capacity",
        "16",
        "--audit-checkpoint-every",
        "5m",
        "--audit-signing-key-path",
        "/etc/krabka/audit.pk8",
        "--audit-signing-key-id",
        "audit-2026",
    ])
    .expect("every flag is valid");
    let from_environment = temp_env::with_vars(
        [
            ("KRABKA_AUDIT_TOPIC", Some("krabka-audit")),
            ("KRABKA_AUDIT_BOOTSTRAP", Some("broker:9092")),
            ("KRABKA_AUDIT_PARTITION", Some("3")),
            (
                "KRABKA_AUDIT_SPOOL_DIR",
                Some("/var/lib/krabka/audit-spool"),
            ),
            ("KRABKA_AUDIT_SPOOL_MAX", Some("512MiB")),
            ("KRABKA_AUDIT_QUEUE_CAPACITY", Some("16")),
            ("KRABKA_AUDIT_CHECKPOINT_EVERY", Some("5m")),
            (
                "KRABKA_AUDIT_SIGNING_KEY_PATH",
                Some("/etc/krabka/audit.pk8"),
            ),
            ("KRABKA_AUDIT_SIGNING_KEY_ID", Some("audit-2026")),
        ],
        || Cli::try_parse_from(["krabka-audit-test"]).map(|cli| cli.audit),
    )
    .expect("every variable is valid");

    check!(from_flags == expected);
    check!(from_environment == expected);
    check!(expected.is_enabled());
}

#[test]
fn invalid_audit_flags_are_refused() {
    let cases: [(&str, &[&str], ErrorKind); 9] = [
        (
            "empty topic",
            &["--audit-topic", ""],
            ErrorKind::InvalidValue,
        ),
        (
            "empty bootstrap",
            &["--audit-bootstrap", ""],
            ErrorKind::InvalidValue,
        ),
        (
            "negative partition",
            &["--audit-partition=-1"],
            ErrorKind::ValueValidation,
        ),
        (
            "zero spool cap",
            &["--audit-spool-max", "0B"],
            ErrorKind::ValueValidation,
        ),
        (
            "spool cap with no unit",
            &["--audit-spool-max", "1024"],
            ErrorKind::ValueValidation,
        ),
        (
            "zero queue",
            &["--audit-queue-capacity", "0"],
            ErrorKind::ValueValidation,
        ),
        (
            "zero checkpoint interval",
            &["--audit-checkpoint-every", "0s"],
            ErrorKind::ValueValidation,
        ),
        (
            "key path with no key id",
            &["--audit-signing-key-path", "/etc/krabka/audit.pk8"],
            ErrorKind::MissingRequiredArgument,
        ),
        (
            "key id with no key path",
            &["--audit-signing-key-id", "audit-2026"],
            ErrorKind::MissingRequiredArgument,
        ),
    ];

    for (name, argv, kind) in cases {
        let result = parse(argv);
        check!(result.map_err(|error| error.kind()) == Err(kind), "{name}");
    }
}

#[test]
fn the_audit_bootstrap_flag_wins_over_the_service_bootstrap() {
    let with_flag = AuditArgs {
        bootstrap: Some("audit-broker:9092".to_owned()),
        ..enabled_args()
    };
    let cases = [
        (
            "flag and service",
            &with_flag,
            Some("wal-broker:9092"),
            Ok("audit-broker:9092"),
        ),
        ("flag only", &with_flag, None, Ok("audit-broker:9092")),
        (
            "service only",
            &enabled_args(),
            Some("wal-broker:9092"),
            Ok("wal-broker:9092"),
        ),
        ("neither", &enabled_args(), None, Err(())),
    ];

    for (name, args, service_bootstrap, expected) in cases {
        let resolved = args.resolve_bootstrap(service_bootstrap);
        check!(
            resolved.as_ref().map_err(|_| ()).copied() == expected,
            "{name}"
        );
        if expected.is_err() {
            assert!(let Err(AuditBuildError::MissingBootstrap) = resolved);
        }
    }
}

#[test]
fn each_constructor_builds_the_exact_event() {
    let events = three_events();
    let expected = [
        AuditEvent::AdminOperation {
            outcome: AuditOutcome::Success,
            principal: AuditPrincipal {
                name: "alice".to_owned(),
                auth_method: "basic".to_owned(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.7".to_owned(),
                port: 51000,
            },
            operation: "delete_request.create".to_owned(),
            resources: vec![
                AuditResource {
                    resource_type: "tenant".to_owned(),
                    name: "tenant-a".to_owned(),
                },
                AuditResource {
                    resource_type: "delete_request".to_owned(),
                    name: "request-1".to_owned(),
                },
            ],
            time_ms: START_MS,
        },
        AuditEvent::AuthorizationDenied {
            principal: AuditPrincipal {
                name: "alice".to_owned(),
                auth_method: "basic".to_owned(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.7".to_owned(),
                port: 51000,
            },
            resource_type: "tenant".to_owned(),
            resource_name: "tenant-b".to_owned(),
            operation: "tenant.read".to_owned(),
            time_ms: START_MS + 1,
        },
        AuditEvent::Authentication {
            outcome: AuditOutcome::Failure,
            mechanism: "bearer".to_owned(),
            principal: AuditPrincipal {
                name: "<unauthenticated>".to_owned(),
                auth_method: "none".to_owned(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.7".to_owned(),
                port: 51000,
            },
            reason: Some("unknown token".to_owned()),
            time_ms: START_MS + 2,
        },
    ];

    check!(events == expected);
}

#[test]
fn a_source_endpoint_gives_one_address_form_for_each_client() {
    let cases = [
        ("ipv4", "10.0.0.7:51000", "10.0.0.7", 51000),
        ("ipv6", "[2001:db8::7]:443", "2001:db8::7", 443),
        ("ipv4 in ipv6 form", "[::ffff:10.0.0.7]:80", "10.0.0.7", 80),
    ];

    for (name, address, ip, port) in cases {
        let address: SocketAddr = address.parse().expect("the address is valid");
        check!(
            source_endpoint(address)
                == AuditEndpoint {
                    ip: ip.to_owned(),
                    port,
                },
            "{name}"
        );
    }
    check!(
        unknown_source_endpoint()
            == AuditEndpoint {
                ip: "0.0.0.0".to_owned(),
                port: 0,
            }
    );
}

#[test]
fn the_product_is_krabka_and_the_named_service() {
    let product = krabka_product("krabka-observability", "1.2.3");

    check!(
        (product.vendor_name, product.name, product.version)
            == (
                "Krabka".to_owned(),
                "krabka-observability".to_owned(),
                "1.2.3".to_owned()
            )
    );
}

#[test]
fn every_closed_set_holds_distinct_names_in_one_form() {
    let dotted = regex::Regex::new(r"^[a-z_]+(\.[a-z_]+)+$").expect("the pattern is valid");
    let plain = regex::Regex::new(r"^[a-z_]+$").expect("the pattern is valid");
    let sets: [(&str, &[&str], &regex::Regex); 3] = [
        ("operations", &OPERATIONS, &dotted),
        ("resource types", &RESOURCE_TYPES, &plain),
        ("mechanisms", &MECHANISMS, &plain),
    ];

    for (name, set, form) in sets {
        let distinct: std::collections::BTreeSet<_> = set.iter().collect();
        check!(distinct.len() == set.len(), "{name} holds a duplicate");
        for value in set {
            check!(form.is_match(value), "{name}: {value}");
        }
    }
}

#[tokio::test]
async fn each_handle_method_stamps_the_clock_time_and_queues_the_event() {
    let time = mock_time();
    let (log, mut queue) = AuditLog::new(8);
    let handle = AuditHandle::new(log, Arc::new(AuditStats::new()), Arc::new(time.clock()));

    handle.admin_operation(
        alice(),
        client(),
        OPERATION_LOG_LEVEL_SET,
        vec![resource(RESOURCE_LOG_LEVEL, "debug")],
        AuditOutcome::Success,
    );
    time.advance(std::time::Duration::from_millis(5));
    handle.authorization_denied(
        alice(),
        client(),
        RESOURCE_WAL_TOPIC,
        "krabka-logs",
        OPERATION_TENANT_WRITE,
    );
    handle.authentication(
        AuditOutcome::Success,
        MECHANISM_MTLS,
        principal("ingester-1", MECHANISM_MTLS),
        client(),
        None,
    );

    let mut queued = Vec::new();
    while let Ok(event) = queue.try_recv() {
        queued.push(event);
    }
    check!(
        queued
            == vec![
                admin_operation(
                    alice(),
                    client(),
                    OPERATION_LOG_LEVEL_SET,
                    vec![resource(RESOURCE_LOG_LEVEL, "debug")],
                    AuditOutcome::Success,
                    EpochMs(START_MS),
                ),
                authorization_denied(
                    alice(),
                    client(),
                    RESOURCE_WAL_TOPIC,
                    "krabka-logs",
                    OPERATION_TENANT_WRITE,
                    EpochMs(START_MS + 5),
                ),
                authentication(
                    AuditOutcome::Success,
                    MECHANISM_MTLS,
                    principal("ingester-1", MECHANISM_MTLS),
                    client(),
                    None,
                    EpochMs(START_MS + 5),
                ),
            ]
    );
    check!(
        (handle.is_enabled(), handle.now(), handle.dropped()) == (true, EpochMs(START_MS + 5), 0)
    );
}

/// The listeners report their decisions through `SecurityEvents`, and the
/// audit log is the one implementation a service installs. Each decision must
/// become the event a reader of the trail expects, with the credential kind
/// that authenticated the principal. A successful authentication must record
/// nothing: one event per request would bury the refusals under the queries.
#[test]
fn the_security_decisions_of_a_listener_become_audit_events() {
    use crate::server_security::{AuthFailureReason, AuthMethod, SecurityEvents};

    let time = mock_time();
    let (log, mut queue) = AuditLog::new(8);
    let handle = AuditHandle::new(log, Arc::new(AuditStats::new()), Arc::new(time.clock()));
    let tenant = krabka_blockstore::TenantId::new("tenant-b").expect("a valid tenant id");
    let source: SocketAddr = "10.0.0.7:51000".parse().expect("the address is valid");

    handle.authentication_failed(
        Some(source),
        Some(AuthMethod::Bearer),
        AuthFailureReason::UnknownCredential,
    );
    handle.authentication_failed(None, None, AuthFailureReason::MissingCredential);
    handle.authentication_succeeded(Some(source), "grafana", AuthMethod::Basic);
    handle.tenant_denied("grafana", AuthMethod::Basic, &tenant);
    handle.admin_denied("ingester-1", AuthMethod::ClientCertificate);

    let mut queued = Vec::new();
    while let Ok(event) = queue.try_recv() {
        queued.push(event);
    }
    check!(
        queued
            == vec![
                authentication(
                    AuditOutcome::Failure,
                    MECHANISM_BEARER,
                    unauthenticated_principal(),
                    source_endpoint(source),
                    Some("unknown_credential".to_owned()),
                    EpochMs(START_MS),
                ),
                authentication(
                    AuditOutcome::Failure,
                    MECHANISM_NONE,
                    unauthenticated_principal(),
                    unknown_source_endpoint(),
                    Some("missing_credential".to_owned()),
                    EpochMs(START_MS),
                ),
                authorization_denied(
                    principal("grafana", MECHANISM_BASIC),
                    unknown_source_endpoint(),
                    RESOURCE_TENANT,
                    "tenant-b",
                    OPERATION_TENANT_ACCESS,
                    EpochMs(START_MS),
                ),
                authorization_denied(
                    principal("ingester-1", MECHANISM_MTLS),
                    unknown_source_endpoint(),
                    RESOURCE_ADMIN_API,
                    "",
                    OPERATION_ADMIN_ACCESS,
                    EpochMs(START_MS),
                ),
            ]
    );
}

/// An audit record must name the principal the authentication layer decided
/// on, and a request on a server without a credentials file must name no
/// principal that a credentials file could also define.
#[test]
fn a_request_principal_becomes_the_audit_principal_it_names() {
    use crate::server_security::{
        AuthMethod, NoSecurityEvents, Principal, SecurityEventSink, TenantGrant,
    };

    let authenticated = |method| Principal::Authenticated {
        name: Arc::from("grafana"),
        method,
        tenants: TenantGrant::All,
        admin: false,
        events: SecurityEventSink::new(Arc::new(NoSecurityEvents)),
    };
    let cases = [
        (
            "unauthenticated",
            Principal::Unauthenticated,
            unauthenticated_principal(),
        ),
        (
            "bearer",
            authenticated(AuthMethod::Bearer),
            principal("grafana", MECHANISM_BEARER),
        ),
        (
            "basic",
            authenticated(AuthMethod::Basic),
            principal("grafana", MECHANISM_BASIC),
        ),
        (
            "client certificate",
            authenticated(AuthMethod::ClientCertificate),
            principal("grafana", MECHANISM_MTLS),
        ),
    ];
    for (name, request_principal, expected) in cases {
        check!(audit_principal_of(&request_principal) == expected, "{name}");
    }
}

#[tokio::test]
async fn a_disabled_audit_layer_spawns_no_writer_and_discards_events() {
    // The bootstrap names a port nothing listens on: a disabled layer must not
    // try it.
    let started = AuditService::start(
        &default_args(),
        product(),
        Some("127.0.0.1:1"),
        None,
        CancellationToken::new(),
    )
    .await
    .expect("a disabled layer always starts");

    for (name, service) in [
        ("started from default flags", started),
        ("built disabled", AuditService::disabled()),
    ] {
        let (handle, writer) = service.into_parts();
        for event in three_events() {
            handle.emit(event);
        }
        check!(
            (writer.is_none(), handle.is_enabled(), handle.dropped()) == (true, false, 0),
            "{name}"
        );
    }
}

#[tokio::test]
async fn an_audit_layer_with_bad_flags_does_not_start() {
    let not_a_directory = tempfile::NamedTempFile::new().expect("temp file");
    let spool_dir = not_a_directory.path().join("spool");
    let missing_key = PathBuf::from("/nonexistent/krabka/audit.pk8");

    let no_bootstrap = AuditService::start(
        &enabled_args(),
        product(),
        None,
        None,
        CancellationToken::new(),
    )
    .await;
    assert!(let Err(AuditBuildError::MissingBootstrap) = no_bootstrap);

    let key_without_id = AuditService::start_with_sink(
        &AuditArgs {
            signing_key_path: Some(missing_key.clone()),
            ..enabled_args()
        },
        product(),
        Arc::new(MemorySink::default()),
        clocks(&mock_time()),
        CancellationToken::new(),
    );
    assert!(let Err(AuditBuildError::IncompleteSigningKey) = key_without_id);

    let unreadable_key = AuditService::start_with_sink(
        &AuditArgs {
            signing_key_path: Some(missing_key.clone()),
            signing_key_id: Some("audit-2026".to_owned()),
            ..enabled_args()
        },
        product(),
        Arc::new(MemorySink::default()),
        clocks(&mock_time()),
        CancellationToken::new(),
    );
    assert!(let Err(AuditBuildError::SigningKey { path, .. }) = unreadable_key);
    check!(path == missing_key);

    let unopenable_spool = AuditService::start_with_sink(
        &AuditArgs {
            spool_dir: Some(spool_dir.clone()),
            ..enabled_args()
        },
        product(),
        Arc::new(MemorySink::default()),
        clocks(&mock_time()),
        CancellationToken::new(),
    );
    assert!(let Err(AuditBuildError::Spool { dir, .. }) = unopenable_spool);
    check!(dir == spool_dir);
}

#[tokio::test]
async fn events_reach_the_sink_in_order_on_one_valid_chain() {
    let time = mock_time();
    let sink = Arc::new(MemorySink::default());
    let shutdown = CancellationToken::new();
    let (handle, writer) = AuditService::start_with_sink(
        &enabled_args(),
        product(),
        Arc::clone(&sink) as Arc<dyn AuditSink>,
        clocks(&time),
        shutdown.clone(),
    )
    .expect("the layer starts")
    .into_parts();
    let events = three_events();

    for event in &events {
        handle.emit(event.clone());
    }
    shutdown.cancel();
    writer
        .expect("an enabled layer has a writer")
        .await
        .expect("the writer does not panic");

    let records = sink.records();
    check!(records == expected_records(&events, ChainState::new()));
    check_chain(&records);
    check!(handle.dropped() == 0);
}

#[tokio::test]
async fn a_failing_sink_spools_records_and_replays_them_in_order() {
    let time = mock_time();
    let spool_dir = tempfile::tempdir().expect("spool dir");
    let sink = Arc::new(FailableSink::default());
    sink.fail.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let (handle, writer) = AuditService::start_with_sink(
        &AuditArgs {
            spool_dir: Some(spool_dir.path().to_path_buf()),
            ..enabled_args()
        },
        product(),
        Arc::clone(&sink) as Arc<dyn AuditSink>,
        clocks(&time),
        shutdown.clone(),
    )
    .expect("the layer starts")
    .into_parts();
    let events = three_events();

    for event in &events {
        handle.emit(event.clone());
    }
    await_until("three records spooled", || handle.stats().spooled() == 3).await;
    check!(sink.inner.records().is_empty());
    check!(handle.stats().depth() == 3);

    sink.fail.store(false, Ordering::SeqCst);
    time.advance(AUDIT_SPOOL_REPLAY_EVERY.to_std());
    await_until("the spool drained", || handle.stats().depth() == 0).await;
    shutdown.cancel();
    writer
        .expect("an enabled layer has a writer")
        .await
        .expect("the writer does not panic");

    let records = sink.inner.records();
    check!(records == expected_records(&events, ChainState::new()));
    check_chain(&records);
    check!((handle.stats().replayed(), handle.dropped()) == (3, 0));
}

#[tokio::test]
async fn a_restarted_writer_goes_on_from_the_chain_in_its_spool() {
    let time = mock_time();
    let spool_dir = tempfile::tempdir().expect("spool dir");
    let args = AuditArgs {
        spool_dir: Some(spool_dir.path().to_path_buf()),
        ..enabled_args()
    };
    let events = three_events();

    // The first process writes two records while the topic is down, and stops.
    let down = Arc::new(FailableSink::default());
    down.fail.store(true, Ordering::SeqCst);
    let first_shutdown = CancellationToken::new();
    let (first, first_writer) = AuditService::start_with_sink(
        &args,
        product(),
        Arc::clone(&down) as Arc<dyn AuditSink>,
        clocks(&time),
        first_shutdown.clone(),
    )
    .expect("the first layer starts")
    .into_parts();
    first.emit(events[0].clone());
    first.emit(events[1].clone());
    await_until("two records spooled", || first.stats().spooled() == 2).await;
    first_shutdown.cancel();
    first_writer
        .expect("an enabled layer has a writer")
        .await
        .expect("the writer does not panic");

    // The next process finds them in the spool. Its own record goes after
    // them on the same chain, and all three replay in order.
    let up = Arc::new(MemorySink::default());
    let second_shutdown = CancellationToken::new();
    let (second, second_writer) = AuditService::start_with_sink(
        &args,
        product(),
        Arc::clone(&up) as Arc<dyn AuditSink>,
        clocks(&time),
        second_shutdown.clone(),
    )
    .expect("the second layer starts")
    .into_parts();
    second.emit(events[2].clone());
    await_until("the third record spooled", || second.stats().spooled() == 1).await;
    time.advance(AUDIT_SPOOL_REPLAY_EVERY.to_std());
    await_until("the spool drained", || second.stats().depth() == 0).await;
    second_shutdown.cancel();
    second_writer
        .expect("an enabled layer has a writer")
        .await
        .expect("the writer does not panic");

    let records = up.records();
    check!(records == expected_records(&events, ChainState::new()));
    check_chain(&records);
}

#[tokio::test]
async fn a_full_queue_drops_events_and_never_blocks_the_caller() {
    let time = mock_time();
    let shutdown = CancellationToken::new();
    let (handle, writer) = AuditService::start_with_sink(
        &AuditArgs {
            queue_capacity: NonZeroUsize::new(1).expect("1 is not zero"),
            ..enabled_args()
        },
        product(),
        Arc::new(StuckSink),
        clocks(&time),
        shutdown.clone(),
    )
    .expect("the layer starts")
    .into_parts();

    // The writer task has not run yet on this single-threaded runtime, so the
    // queue holds exactly one event and each later emit must return at once.
    let event = three_events().remove(0);
    for _ in 0..100 {
        handle.emit(event.clone());
    }

    check!(handle.dropped() == 99);
    writer.expect("an enabled layer has a writer").abort();
}

#[tokio::test]
async fn an_event_after_shutdown_counts_as_dropped() {
    let time = mock_time();
    let sink = Arc::new(MemorySink::default());
    let shutdown = CancellationToken::new();
    let (handle, writer) = AuditService::start_with_sink(
        &enabled_args(),
        product(),
        Arc::clone(&sink) as Arc<dyn AuditSink>,
        clocks(&time),
        shutdown.clone(),
    )
    .expect("the layer starts")
    .into_parts();

    shutdown.cancel();
    writer
        .expect("an enabled layer has a writer")
        .await
        .expect("the writer does not panic");
    handle.emit(three_events().remove(0));

    check!((handle.dropped(), sink.records().len()) == (1, 0));
}

#[tokio::test]
async fn a_signing_key_signs_a_last_checkpoint_over_the_chain() {
    let time = mock_time();
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).expect("an Ed25519 key");
    let key_dir = tempfile::tempdir().expect("key dir");
    let key_path = key_dir.path().join("audit.pk8");
    std::fs::write(&key_path, key.serialize_der()).expect("write the key");
    let sink = Arc::new(MemorySink::default());
    let shutdown = CancellationToken::new();
    let (handle, writer) = AuditService::start_with_sink(
        &AuditArgs {
            signing_key_path: Some(key_path),
            signing_key_id: Some("audit-test".to_owned()),
            ..enabled_args()
        },
        product(),
        Arc::clone(&sink) as Arc<dyn AuditSink>,
        clocks(&time),
        shutdown.clone(),
    )
    .expect("the layer starts")
    .into_parts();
    let events = &three_events()[..2];

    for event in events {
        handle.emit(event.clone());
    }
    shutdown.cancel();
    writer
        .expect("an enabled layer has a writer")
        .await
        .expect("the writer does not panic");

    let records = sink.records();
    assert!(records.len() == 3);
    let mut chain = ChainState::new();
    check!(records[..2] == expected_records(events, chain.clone()));
    for record in &records[..2] {
        chain.extend(&record.value);
    }
    let value: serde_json::Value =
        serde_json::from_slice(&records[2].value).expect("the checkpoint is JSON");
    let checkpoint = Checkpoint::from_value(&value).expect("a checkpoint");
    check!(
        (
            records[2].class,
            checkpoint.key_id.as_str(),
            checkpoint.seq_high,
            checkpoint.chain_head,
            checkpoint.verify(key.public_key_raw()),
        ) == (
            AuditEventClass::Checkpoint,
            "audit-test",
            Seq(1),
            chain.head(),
            true
        )
    );
}

#[test]
fn an_audit_record_becomes_a_kafka_record_pinned_to_its_partition() {
    let record = AuditRecord {
        class: AuditEventClass::ApiActivity,
        value: br#"{"class_uid":6003}"#.to_vec(),
        headers: vec![
            ("event_class".to_owned(), b"api_activity".to_vec()),
            ("seq".to_owned(), b"7".to_vec()),
        ],
    };

    check!(
        audit_producer_record("krabka-audit", PartitionIndex(2), record)
            == ProducerRecord {
                topic: "krabka-audit".to_owned(),
                partition: Some(2),
                key: None,
                value: Some(Bytes::from_static(br#"{"class_uid":6003}"#)),
                headers: vec![
                    Header {
                        key: "event_class".to_owned(),
                        value: Some(Bytes::from_static(b"api_activity")),
                    },
                    Header {
                        key: "seq".to_owned(),
                        value: Some(Bytes::from_static(b"7")),
                    },
                ],
                timestamp_ms: None,
            }
    );
}

//! One process, `--target all`, with a profile going in at the ingest door and
//! coming back out at the query door -- and a `SIGTERM` that has to end it.
//!
//! The interesting failure this suite exists for is not "the process does not
//! start". It is a `--target all` that starts, binds, passes its readiness
//! probe, accepts every push and answers every query with nothing, because the
//! block builder and the read path each built an object store of their own and
//! the blocks one wrote are in a store the other never opens. Nothing in the
//! logs says so and nothing in the metrics says so: the only symptom is an
//! empty flamegraph, which is also what "no profiles matched" looks like.
//!
//! So the store here is `memory:///`, where two stores really are two
//! universes, and the profile that is asserted on is one the hot tier cannot
//! answer from. It reaches the querier only by having been written to a block
//! and read back out of the same store.
//!
//! Both tests need a real child process -- one to hold a port open while this
//! one pushes to it, one to carry an exit status a signal can be judged by --
//! and the child is a re-execution of this test binary rather than the
//! `krabka-profiles` binary. `env!("CARGO_BIN_EXE_*")` is Cargo's alone, and
//! `bazel test //...` is this repository's gate; re-executing
//! [`std::env::current_exe`] builds under both, and the child then calls the
//! binary's own [`run`] rather than shelling out, so what is under test is the
//! real role composition.

use std::{
    collections::BTreeMap,
    io::Write as _,
    process::{Child, Command},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use clap::Parser as _;
use flate2::{Compression, write::GzEncoder};
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_pprof::{PprofProfile, proto};
use krabka_profiles::PROFILES_WAL_TOPIC;
use serde_json::{Value, json};

use super::{Cli, run};

/// Set on the child re-execution, and carries the broker the child's roles
/// speak to. One marker per test, because both spawn the same executable and
/// each child has to know which of them it is.
const INGEST_CHILD: &str = "KRABKA_PROFILES_TEST_ALL_INGEST_BOOTSTRAP";
const SIGTERM_CHILD: &str = "KRABKA_PROFILES_TEST_ALL_SIGTERM_BOOTSTRAP";
/// Where the child binds its Pyroscope port and its admin port.
const CHILD_LISTEN: &str = "KRABKA_PROFILES_TEST_ALL_LISTEN";
const CHILD_ADMIN: &str = "KRABKA_PROFILES_TEST_ALL_ADMIN";

const INGEST_TEST: &str = "all_in_one::a_push_at_the_ingest_door_is_answered_at_the_query_door";
const SIGTERM_TEST: &str = "all_in_one::a_sigterm_stops_every_role_and_the_process_exits_cleanly";

const TENANT: &str = "tenant-a";
const PROFILE_NAME: &str = "process_cpu";
const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
/// The profile the assertions are about. Its timestamp is an hour old, which
/// is what puts it beyond the hot tier's retention.
const ARCHIVED_SERVICE: &str = "checkout";
/// Pushed in the same request, and timestamped now. The hot store's retention
/// horizon is `newest retained record - --hot-store-max-age`, so this record
/// is what makes the older one unreachable from the WAL tail -- and a block in
/// the shared object store the only place it can be answered from.
const RECENT_SERVICE: &str = "heartbeat";
const FUNC_WORK: &str = "main.work";
const FUNC_HOT: &str = "main.hotloop";
const LEAF_VALUE: i64 = 100;
const SELF_VALUE: i64 = 40;
const ARCHIVED_AGE: Duration = Duration::from_hours(1);

/// Flags that make the block path prompt enough to assert on, and the hot tier
/// short enough that it cannot be the one answering.
const INGEST_FLAGS: &[&str] = &[
    // A block per record, published promptly, and reloaded promptly: the test
    // asserts on what reaches the read path, not on how long the default flush
    // rules take to get it there.
    "--block-builder-flush-records",
    "1",
    "--block-builder-flush-max-age",
    "1s",
    "--index-refresh-interval",
    "500ms",
    // One second of hot tier, so the hour-old profile can only be answered
    // from a block.
    "--hot-store-max-age",
    "1s",
];

/// A push at `push.v1.PusherService/Push` and a query at `/pyroscope/render`,
/// over real HTTP, against one `--target all` process.
///
/// The two doors are the point: the push route belongs to the distributor's
/// router and the render route to the query surface, and in `--target all`
/// they are merged onto the one Pyroscope port. A regression that split them
/// across two listeners would show up here as a connection refused rather than
/// as a silently different deployment shape.
#[test]
fn a_push_at_the_ingest_door_is_answered_at_the_query_door() {
    if let Ok(bootstrap) = std::env::var(INGEST_CHILD) {
        run_all_child(&bootstrap, INGEST_FLAGS);
        return;
    }

    let dir = tempfile::tempdir().expect("temporary directory");
    let runtime = parent_runtime();
    let broker = start_broker(&runtime, dir.path());
    let bootstrap = broker.listen_addr().to_string();
    let listen = free_loopback_addr();
    let admin = free_loopback_addr();
    let _serving = spawn_all_child(INGEST_TEST, INGEST_CHILD, &bootstrap, &listen, &admin);

    runtime.block_on(async {
        // Every role's gates, on the one port, before anything is pushed. A
        // push accepted by a process whose block builder had not yet reached
        // the broker would be a race this suite could not tell from a bug.
        wait_until_ready(&listen, Duration::from_secs(90)).await;

        let now_ms = epoch_millis();
        let archived_ms =
            now_ms - i64::try_from(ARCHIVED_AGE.as_millis()).expect("an hour in millis");

        // One request, two series, in this order. The hot store prunes once
        // per batch of WAL records applied, so the archived series is dropped
        // by the same apply that admitted it and is never answerable from the
        // WAL tail at any instant this test could observe. The one place left
        // for it to come from is a block in the object store the block builder
        // wrote to -- which is the point.
        push(
            &listen,
            &[(ARCHIVED_SERVICE, archived_ms), (RECENT_SERVICE, now_ms)],
        )
        .await;

        let render = render_until_answered(
            &listen,
            ARCHIVED_SERVICE,
            archived_ms - 60_000,
            archived_ms + 60_000,
            Duration::from_secs(90),
        )
        .await;

        check!(flame_names(&render) == vec![FUNC_HOT.to_string(), FUNC_WORK.to_string()]);
        check!(flame_ticks(&render) == Some(LEAF_VALUE + SELF_VALUE));
        check!(
            render.pointer("/metadata/units").and_then(Value::as_str) == Some("nanoseconds"),
            "render metadata must carry the profile type's unit, got {render}"
        );
    });
}

/// A `SIGTERM` to `--target all` has to end the process, and end it cleanly.
///
/// Six roles stop one at a time here, each waited for before the next is asked
/// to stop. A role that ignores its token is not skipped; it is waited for,
/// and holds the whole stop open until `--all-drain-stage-timeout` expires.
/// Six of those in a row outlast any orchestrator's grace period, and the
/// symptom is a container that takes minutes to die and reports exit 137.
///
/// `code()` is `Some(0)` only for a process that returned from `main`. A
/// process a signal killed reports `None`, which is exactly what a stop that
/// never finished leaves an orchestrator holding.
#[test]
fn a_sigterm_stops_every_role_and_the_process_exits_cleanly() {
    if let Ok(bootstrap) = std::env::var(SIGTERM_CHILD) {
        run_all_child(&bootstrap, &[]);
        return;
    }

    let dir = tempfile::tempdir().expect("temporary directory");
    let runtime = parent_runtime();
    let broker = start_broker(&runtime, dir.path());
    let bootstrap = broker.listen_addr().to_string();
    let listen = free_loopback_addr();
    let admin = free_loopback_addr();
    let mut serving = spawn_all_child(SIGTERM_TEST, SIGTERM_CHILD, &bootstrap, &listen, &admin);

    runtime.block_on(async {
        // Signalled only once every role is up. A stop that arrived mid-start
        // would exercise the start's own cancellation paths instead of the
        // drain.
        wait_until_ready(&listen, Duration::from_secs(90)).await;
        // Something in the WAL, so the block builder has a partition
        // assignment and an offset to commit rather than nothing to drain.
        push(&listen, &[(RECENT_SERVICE, epoch_millis())]).await;
    });

    // Through `sh` rather than a `kill` binary: the shell builtin is always
    // there, including inside a Bazel test sandbox, and `unsafe_code` is
    // forbidden workspace-wide so `libc::kill` is not an option.
    let signalled = Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -TERM {}", serving.0.id()))
        .status()
        .expect("send SIGTERM");
    assert!(signalled.success());

    let status = serving.wait_for_exit(Duration::from_mins(1));

    assert!(status.code() == Some(0), "`--target all` exited {status}");
}

/// The role under test, in the child: the binary's own `run`, on the real
/// `--target all` composition, built from a real `Cli`.
fn run_all_child(bootstrap: &str, flags: &[&str]) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    // Registered before the parent can see anything this process exports, so
    // the parent's `kill` cannot land in the window before the roles install
    // their own. Tokio's handlers are process-wide and refcounted, so the one
    // installed later is this same registration.
    let _terminate = runtime
        .block_on(async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        })
        .expect("install SIGTERM handler");

    let listen = std::env::var(CHILD_LISTEN).expect("child listen address");
    let admin = std::env::var(CHILD_ADMIN).expect("child admin address");
    let mut argv: Vec<String> = [
        "krabka-profiles",
        "--target",
        "all",
        "--bootstrap",
        bootstrap,
        "--listen",
        &listen,
        "--admin-listen-addr",
        &admin,
        // Two object stores in one process would be two universes.
        "--object-store-url",
        "memory:///",
        "--wal-poll-timeout",
        "200ms",
        // Further out than either test can run, so no compaction pass rewrites
        // the block a query is about.
        "--compactor-interval",
        "1h",
    ]
    .iter()
    .map(|argument| (*argument).to_string())
    .collect();
    argv.extend(flags.iter().map(|flag| (*flag).to_string()));

    let cli = Cli::try_parse_from(argv).expect("child CLI");
    runtime.block_on(async { run(cli).await.expect("`--target all` returns on SIGTERM") });
}

/// Re-executes this test binary as the named test, with the environment that
/// makes it the child rather than the parent.
fn spawn_all_child(
    test: &str,
    marker: &str,
    bootstrap: &str,
    listen: &str,
    admin: &str,
) -> ChildGuard {
    ChildGuard(
        Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", test, "--nocapture"])
            .env(marker, bootstrap)
            .env(CHILD_LISTEN, listen)
            .env(CHILD_ADMIN, admin)
            .env("RUST_LOG", "warn")
            .spawn()
            .expect("spawn the `--target all` child"),
    )
}

/// Kills the process whatever the test does, including panicking out of an
/// assertion. A leaked `--target all` holds its port and its consumer group,
/// and the next run of this suite would fail for reasons that have nothing to
/// do with the code under test.
struct ChildGuard(Child);

impl ChildGuard {
    fn wait_for_exit(&mut self, within: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if let Some(status) = self.0.try_wait().expect("poll the child") {
                return status;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("`--target all` did not exit within {within:?} of SIGTERM");
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A multi-thread runtime, because the broker keeps serving from its own tasks
/// while this thread blocks on the child.
fn parent_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("parent runtime")
}

fn start_broker(runtime: &tokio::runtime::Runtime, dir: &std::path::Path) -> BrokerHandle {
    runtime.block_on(async {
        let broker = Broker::start(BrokerConfig::for_tests(dir.join("broker")))
            .await
            .expect("broker start");
        create_wal_topic(&broker.listen_addr().to_string()).await;
        broker
    })
}

fn epoch_millis() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_millis(),
    )
    .expect("epoch milliseconds fit in i64")
}

async fn create_wal_topic(bootstrap: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: PROFILES_WAL_TOPIC.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            krabka_units::secs(5),
        )
        .await
        .expect("create the profiles WAL topic");
}

/// An address nothing is listening on yet. The child's ports have to be named
/// before it starts, because the parent connects to them.
fn free_loopback_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free port");
    listener
        .local_addr()
        .expect("the bound address")
        .to_string()
}

/// Polls `/ready` on the Pyroscope port until every role's gates are met.
///
/// Bounded rather than slept through: a fixed sleep either fails on a loaded
/// machine or wastes the same seconds on an idle one, and neither reports what
/// the process was still waiting for. The body of the last 503 does.
async fn wait_until_ready(listen: &str, within: Duration) {
    let url = format!("http://{listen}/ready");
    let deadline = Instant::now() + within;
    let mut last = String::from("nothing answered the readiness probe");
    while Instant::now() < deadline {
        if let Ok(response) = reqwest::get(&url).await {
            let ready = response.status().is_success();
            last = response.text().await.unwrap_or_default();
            if ready {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("`--target all` was not ready within {within:?}: {last}");
}

/// Pushes one profile per `(service, timestamp)` pair, through the
/// distributor's Connect door, in the order given.
async fn push(listen: &str, series: &[(&str, i64)]) {
    let response = reqwest::Client::new()
        .post(format!("http://{listen}/push.v1.PusherService/Push"))
        .header("content-type", "application/json")
        .header("x-scope-orgid", TENANT)
        .body(serde_json::to_vec(&push_body(series)).expect("serialize the push body"))
        .send()
        .await
        .expect("push over HTTP");

    assert!(
        response.status().is_success(),
        "the push was rejected: {}",
        response.status()
    );
}

/// Renders until the block builder has published the pushed profile and the
/// read path has picked the new index up, or until `within` elapses.
///
/// The answer starts out legitimately empty -- the record is in the WAL before
/// it is in a block, and in a block before the index snapshot naming it has
/// been reloaded -- so an empty flamegraph is a retry rather than a failure.
/// The deadline is what turns "never arrives" back into one.
async fn render_until_answered(
    listen: &str,
    service: &str,
    from_ms: i64,
    until_ms: i64,
    within: Duration,
) -> Value {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(
            "query",
            &format!("{PROFILE_TYPE}{{service_name=\"{service}\"}}"),
        )
        .append_pair("from", &from_ms.to_string())
        .append_pair("until", &until_ms.to_string())
        .finish();
    let url = format!("http://{listen}/pyroscope/render?{query}");
    let deadline = Instant::now() + within;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        if let Ok(response) = reqwest::Client::new()
            .get(&url)
            .header("x-scope-orgid", TENANT)
            .send()
            .await
        {
            let answered = response.status().is_success();
            let body = response.text().await.unwrap_or_default();
            if answered {
                last = serde_json::from_str(&body).unwrap_or(Value::Null);
                if !flame_names(&last).is_empty() {
                    return last;
                }
            } else {
                last = Value::String(body);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("no profile for {service} came back within {within:?}; last answer was {last}");
}

fn flame_names(value: &Value) -> Vec<String> {
    let mut names: Vec<String> = value
        .pointer("/flamebearer/names")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|names| names.iter())
        .filter_map(Value::as_str)
        .filter(|name| *name != "total" && !name.is_empty())
        .map(ToString::to_string)
        .collect();
    names.sort();
    names
}

fn flame_ticks(value: &Value) -> Option<i64> {
    value
        .pointer("/flamebearer/numTicks")
        .or_else(|| value.pointer("/flamebearer/total"))
        .and_then(Value::as_i64)
}

fn push_body(series: &[(&str, i64)]) -> Value {
    let series: Vec<Value> = series
        .iter()
        .map(|(service, at_ms)| {
            json!({
                "labels": [
                    { "name": "__name__", "value": PROFILE_NAME },
                    { "name": "service_name", "value": service }
                ],
                "samples": [{
                    "rawProfile": BASE64.encode(gzip_bytes(&synthetic_cpu_pprof(at_ms * 1_000_000))),
                    "ID": format!("krabka-all-in-one-{service}")
                }]
            })
        })
        .collect();
    json!({ "series": series })
}

/// A two-sample CPU profile: `main.hotloop` called from `main.work`, and
/// `main.work` on its own.
fn synthetic_cpu_pprof(time_nanos: i64) -> Vec<u8> {
    // string_table: 0="" 1="cpu" 2="nanoseconds" 3=main.work 4=main.hotloop 5="app.go"
    let profile = proto::Profile {
        sample_type: vec![proto::ValueType { r#type: 1, unit: 2 }],
        sample: vec![
            proto::Sample {
                location_id: vec![2, 1], // leaf-first: main.hotloop -> main.work
                value: vec![LEAF_VALUE],
                label: Vec::new(),
            },
            proto::Sample {
                location_id: vec![1], // main.work
                value: vec![SELF_VALUE],
                label: Vec::new(),
            },
        ],
        mapping: vec![proto::Mapping {
            id: 1,
            symbolization: proto::MappingSymbolization::from_parts((true, false, false, false)),
            ..Default::default()
        }],
        location: vec![
            proto::Location {
                id: 1,
                mapping_id: 1,
                address: 0x1000,
                line: vec![proto::Line {
                    function_id: 1,
                    line: 10,
                    column: 0,
                }],
                is_folded: false,
            },
            proto::Location {
                id: 2,
                mapping_id: 1,
                address: 0x2000,
                line: vec![proto::Line {
                    function_id: 2,
                    line: 20,
                    column: 0,
                }],
                is_folded: false,
            },
        ],
        function: vec![
            proto::Function {
                id: 1,
                name: 3,
                system_name: 3,
                filename: 5,
                start_line: 1,
            },
            proto::Function {
                id: 2,
                name: 4,
                system_name: 4,
                filename: 5,
                start_line: 2,
            },
        ],
        string_table: vec![
            String::new(),
            "cpu".to_string(),
            "nanoseconds".to_string(),
            FUNC_WORK.to_string(),
            FUNC_HOT.to_string(),
            "app.go".to_string(),
        ],
        time_nanos,
        duration_nanos: 1_000_000_000,
        period_type: Some(proto::ValueType { r#type: 1, unit: 2 }),
        period: 10_000_000,
        ..Default::default()
    };
    PprofProfile::from(profile).encode()
}

fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

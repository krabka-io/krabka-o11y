//! `--target all`: in through the distributor's door, out through the
//! query-frontend's.
//!
//! The composition runs in a child process, and the parent only ever speaks to
//! it over the network. Nothing here reaches into the roles: the spans go in on
//! the OTLP/HTTP ingest port and come back on `--listen`, which is a different
//! port served by a different role, reached through the query-frontend's HTTP
//! fan-out to the querier and the querier's HTTP call to the live-store. Every
//! hop the composition claims to run is a hop this test makes it run.
//!
//! The child is this test binary re-executed, not a path to a built artifact.
//! `CARGO_BIN_EXE_*` names one, but only Cargo defines it, and `bazel test` is
//! this repository's primary gate -- a suite that compiles under one of the two
//! build systems is a suite that is not run. `std::env::current_exe()` is the
//! same trick `krabka-profiles`' `sigterm_exits_the_querier` uses for the same
//! reason, and it buys something beyond portability: because the module lives
//! in the binary crate, the child calls [`run`] directly on a [`Cli`] it
//! parsed, so what is under test is the real role composition rather than
//! whatever a shell-out happened to resolve to.

use std::{
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use clap::Parser as _;
use krabka_broker::{Broker, BrokerConfig};
use krabka_observability::topic_contract::{TRACES_TOPICS, TopicSettings, provision_topics};
use krabka_traces::frontend::TraceByIdResponseJson;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData,
};
use prost::Message as _;

use super::{Cli, run};

/// Set on the child re-execution, and the marker that tells it to be the role
/// rather than the test. It carries the broker the child's WAL clients reach.
const CHILD_BOOTSTRAP: &str = "KRABKA_TRACES_TEST_ALL_BOOTSTRAP";
/// The three ports the parent has to name before the child starts, because the
/// parent dials all three.
const CHILD_TEMPO_API: &str = "KRABKA_TRACES_TEST_ALL_TEMPO_API";
const CHILD_OTLP_HTTP: &str = "KRABKA_TRACES_TEST_ALL_OTLP_HTTP";
const CHILD_ADMIN: &str = "KRABKA_TRACES_TEST_ALL_ADMIN";

const TEST_NAME: &str = "all_in_one_serves_ingest_and_query::\
                         spans_pushed_to_the_ingest_port_come_back_through_the_tempo_api_port";

/// The tenant every request in this suite carries.
const TENANT: &str = "tenant-a";

/// The trace pushed with timestamps a minute in the past, which the live-store
/// evicts as soon as a newer one arrives.
const OLD_TRACE: [u8; 16] = [0xa1; 16];
/// The trace pushed at "now", which is what evicts [`OLD_TRACE`].
const NEW_TRACE: [u8; 16] = [0xb2; 16];

/// One process, every role, and the two doors an operator actually uses.
///
/// The last assertion is what makes this more than a smoke test. Both traces
/// are pushed to the distributor; the newer one is polled for first, and its
/// arrival proves the live-store has appended it -- which, with `--retention 5s`
/// against spans a minute apart, is also the moment the live-store evicts the
/// older one. So when the older trace comes back afterwards it cannot be coming
/// from the live tier. It is coming from a block, which the block-builder wrote
/// and the querier read, and with `--object-store-url memory:///` those are the
/// same heap only if the process built **one** store and handed it to both.
/// Four roles calling `build_object_store` for themselves is four `InMemory`
/// stores: the block builder writes into one, the querier searches another that
/// nothing ever wrote to, and the failure has no symptom at all -- no error, no
/// warning, just a 404 that looks like a trace nobody sent. This test is the
/// symptom.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spans_pushed_to_the_ingest_port_come_back_through_the_tempo_api_port() {
    if let Ok(bootstrap) = std::env::var(CHILD_BOOTSTRAP) {
        run_all_in_one_child(&bootstrap).await;
        return;
    }

    let directory = tempfile::tempdir().expect("temporary directory");
    let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    provision_topics(&bootstrap, &TRACES_TOPICS, &TopicSettings::single_broker())
        .await
        .expect("provision the traces WAL topic");

    let tempo_api = free_loopback_addr();
    let otlp_http = free_loopback_addr();
    let admin = free_loopback_addr();
    let mut process = AllInOne::spawn(&bootstrap, &tempo_api, &otlp_http, &admin);

    let now_ns = unix_nanos();
    let http = reqwest::Client::new();
    let ingest = format!("http://{otlp_http}/v1/traces");

    // The first push doubles as the wait for the composition to be listening:
    // it is retried until the distributor answers, rather than slept for.
    push_until_accepted(
        &http,
        &ingest,
        OLD_TRACE,
        now_ns - 60_000_000_000,
        &mut process,
    )
    .await;
    push_until_accepted(&http, &ingest, NEW_TRACE, now_ns, &mut process).await;

    let query = format!("http://{tempo_api}/api/v2/traces");
    let recent = trace_within_deadline(&http, &query, NEW_TRACE).await;
    check!(
        span_names(&recent) == vec!["child".to_string(), "root".to_string()],
        "the spans pushed are the spans returned"
    );

    let evicted_from_the_live_tier = trace_within_deadline(&http, &query, OLD_TRACE).await;
    check!(
        span_names(&evicted_from_the_live_tier) == vec!["child".to_string(), "root".to_string()],
        "a trace the live-store has dropped is still served, so the querier reads \
         the blocks the block-builder wrote"
    );

    // One `/ready` for every role. Reaching 200 at all is the load-bearing
    // part: the gates come from four different roles registering into one
    // list, and a role that registered a gate nothing ever satisfied -- or a
    // frontend that waited on a querier whose `/ready` was waiting on the
    // frontend -- would leave this 503 for the life of the process while every
    // port stayed open and every query still worked.
    let ready = http
        .get(format!("http://{admin}/ready"))
        .send()
        .await
        .expect("the admin port answers");
    let status = ready.status();
    check!(
        status == reqwest::StatusCode::OK,
        "{}",
        ready.text().await.unwrap_or_default()
    );
}

/// The role under test: the binary's own [`run`], on the `all` arm.
async fn run_all_in_one_child(bootstrap: &str) {
    let tempo_api = std::env::var(CHILD_TEMPO_API).expect("child Tempo API address");
    let otlp_http = std::env::var(CHILD_OTLP_HTTP).expect("child OTLP/HTTP address");
    let admin = std::env::var(CHILD_ADMIN).expect("child admin address");
    let cli = Cli::try_parse_from([
        "krabka-traces",
        "--target",
        "all",
        "--bootstrap",
        bootstrap,
        "--listen",
        &tempo_api,
        "--otlp-http-listen",
        &otlp_http,
        "--admin-listen-addr",
        &admin,
        // Every listener this test never dials is left to the kernel. A fixed
        // default would collide with whatever else is on the machine,
        // including another copy of this test.
        "--grpc-listen",
        "127.0.0.1:0",
        "--jaeger-grpc-listen",
        "127.0.0.1:0",
        "--jaeger-compact-listen",
        "127.0.0.1:0",
        "--jaeger-http-listen",
        "127.0.0.1:0",
        "--zipkin-listen",
        "127.0.0.1:0",
        // In memory, deliberately: see the test's doc comment.
        "--object-store-url",
        "memory:///",
        // A block per poll, a poll per second, and an index refresh on the
        // same tick. The defaults would have the first block appear ten
        // seconds in, which is a long time to hold a test open for no extra
        // coverage.
        "--block-builder-window",
        "1s",
        "--block-builder-flush-max-records",
        "1",
        "--block-builder-flush-max-age",
        "1s",
        // Short enough that a span a minute old is evicted by a span pushed
        // now, which is the whole mechanism of the last assertion.
        "--retention",
        "5s",
    ])
    .expect("child CLI");

    run(cli).await.expect("the all-in-one composition runs");
}

/// The child running the composition, killed when the test ends.
///
/// A test that panics must not leave a process holding three ports and four
/// consumer groups behind it, so the kill is in `Drop` rather than at the end
/// of the test body.
struct AllInOne(std::process::Child);

impl AllInOne {
    fn spawn(bootstrap: &str, tempo_api: &str, otlp_http: &str, admin: &str) -> Self {
        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env(CHILD_BOOTSTRAP, bootstrap)
            .env(CHILD_TEMPO_API, tempo_api)
            .env(CHILD_OTLP_HTTP, otlp_http)
            .env(CHILD_ADMIN, admin)
            .spawn()
            .expect("spawn the all-in-one child");
        Self(child)
    }

    /// Why the child is gone, if it is. A composition that failed to start --
    /// a port already taken, a WAL topic that does not meet the contract --
    /// would otherwise show up as a poll loop running out its whole deadline
    /// against a process that died in the first second.
    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.0.try_wait().expect("poll the child")
    }
}

impl Drop for AllInOne {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Pushes `trace` until the distributor accepts it, or the deadline passes.
///
/// The retry is the readiness wait: the distributor's ingest port refuses
/// connections until the WAL producer has a broker, so a push that connects
/// and returns 200 is proof the write path is up. Polling for that beats
/// sleeping a guess, which is either flaky or slow and usually both.
async fn push_until_accepted(
    http: &reqwest::Client,
    ingest: &str,
    trace: [u8; 16],
    start_ns: i64,
    process: &mut AllInOne,
) {
    let body = otlp_body(trace, start_ns);
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last = String::from("never answered");
    while Instant::now() < deadline {
        match http
            .post(ingest)
            .header("content-type", "application/x-protobuf")
            .header("x-scope-orgid", TENANT)
            .body(body.clone())
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => return,
            Ok(resp) => last = format!("ingest returned {}", resp.status()),
            Err(error) => last = error.to_string(),
        }
        assert!(let None = process.exited(), "the composition exited: {last}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the distributor never accepted a push: {last}");
}

/// Reads one trace back through the query-frontend, retrying until it is there.
///
/// Every hop between the push and this read is asynchronous -- the WAL, the
/// live-store's consumer, the block builder's flush, the querier's index
/// refresh -- so the honest wait is a poll against a deadline. A fixed sleep
/// would have to be longer than the slowest of them on the slowest machine,
/// and would still be a guess.
async fn trace_within_deadline(
    http: &reqwest::Client,
    query: &str,
    trace: [u8; 16],
) -> TraceByIdResponseJson {
    let url = format!("{query}/{}", hex::encode(trace));
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = String::from("never answered");
    while Instant::now() < deadline {
        match http.get(&url).header("x-scope-orgid", TENANT).send().await {
            Ok(resp) if resp.status().is_success() => {
                assert!(let Ok(body) = resp.json::<TraceByIdResponseJson>().await);
                if !body.is_empty() {
                    return body;
                }
                last = "the frontend returned a trace with no spans".to_string();
            }
            Ok(resp) => last = format!("the frontend returned {}", resp.status()),
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{} never came back: {last}", hex::encode(trace));
}

/// The span names in an assembled trace, sorted so the assertion does not
/// depend on the order two tiers happened to be merged in.
fn span_names(body: &TraceByIdResponseJson) -> Vec<String> {
    let mut names: Vec<String> = body
        .trace
        .resource_spans
        .iter()
        .flat_map(|resource| resource.scope_spans.iter())
        .flat_map(|scope| scope.spans.iter())
        .filter_map(|span| span.rest.get("name").and_then(|name| name.as_str()))
        .map(ToString::to_string)
        .collect();
    names.sort();
    names
}

/// An address nothing is listening on yet.
///
/// The child needs its ports named before it starts, because the parent dials
/// all three of them. The listener is bound and dropped rather than guessed
/// at, so the port is one the kernel had free a moment ago instead of one this
/// test hopes is.
fn free_loopback_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener
        .local_addr()
        .expect("the bound address")
        .to_string()
}

fn unix_nanos() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_nanos(),
    )
    .expect("a clock before the year 2262")
}

fn otlp_body(trace_id: [u8; 16], start_ns: i64) -> Vec<u8> {
    let start = u64::try_from(start_ns).expect("a timestamp after 1970");
    TracesData {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![
                    OtlpSpan {
                        trace_id: trace_id.to_vec(),
                        span_id: vec![2; 8],
                        name: "root".into(),
                        start_time_unix_nano: start,
                        end_time_unix_nano: start + 1_000_000,
                        ..OtlpSpan::default()
                    },
                    OtlpSpan {
                        trace_id: trace_id.to_vec(),
                        span_id: vec![3; 8],
                        parent_span_id: vec![2; 8],
                        name: "child".into(),
                        start_time_unix_nano: start + 100_000,
                        end_time_unix_nano: start + 200_000,
                        ..OtlpSpan::default()
                    },
                ],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}

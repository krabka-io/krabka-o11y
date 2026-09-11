//! A real `SIGTERM`, sent to a real profiles querier, must make the process
//! exit.
//!
//! Every orchestrator stops a container by sending `SIGTERM` and waiting out a
//! grace period before it resorts to `SIGKILL`. Installing a handler is not
//! what that measures: a role whose shutdown path waits on a task that never
//! hears the cancel keeps the process alive just as surely as a role with no
//! handler at all, and the only symptom is a stop that takes the whole grace
//! period and reports exit 137. This test runs the real querier composition --
//! the same `run(cli)` the binary runs -- against a real broker, signals it
//! the way an orchestrator would, and asserts on the exit status.
//!
//! `status.code()` is `Some(0)` only for a process that returned from `main`.
//! A process a signal killed reports `None`, which is exactly what the
//! unfixed WAL tail left behind.

use std::{
    collections::BTreeMap,
    process::Command,
    time::{Duration, Instant},
};

use assert2::assert;
use clap::Parser as _;
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_profiles::PROFILES_WAL_TOPIC;

use super::{Cli, run};

/// Set on the child re-execution of this test binary, and carries the broker
/// the child's WAL tail reads.
const CHILD_BOOTSTRAP: &str = "KRABKA_PROFILES_TEST_SIGTERM_BOOTSTRAP";
/// Where the child binds its data port, its admin port, and its blocks.
const CHILD_LISTEN: &str = "KRABKA_PROFILES_TEST_SIGTERM_LISTEN";
const CHILD_ADMIN: &str = "KRABKA_PROFILES_TEST_SIGTERM_ADMIN";
const CHILD_STORE: &str = "KRABKA_PROFILES_TEST_SIGTERM_STORE";

const TEST_NAME: &str = "sigterm_exits_the_querier::sigterm_makes_the_querier_process_exit";

#[test]
fn sigterm_makes_the_querier_process_exit() {
    if let Ok(bootstrap) = std::env::var(CHILD_BOOTSTRAP) {
        run_querier_child(&bootstrap);
        return;
    }

    let dir = tempfile::tempdir().expect("temporary directory");
    let store_root = dir.path().join("blocks");
    std::fs::create_dir_all(&store_root).expect("block directory");

    // A multi-thread runtime, because the broker keeps serving from its own
    // tasks while this thread blocks on the child.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("parent runtime");
    let broker = runtime.block_on(async {
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().join("broker")))
            .await
            .expect("broker start");
        create_wal_topic(&broker.listen_addr().to_string()).await;
        broker
    });
    let bootstrap = broker.listen_addr().to_string();

    let listen = free_loopback_addr();
    let admin = free_loopback_addr();
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_BOOTSTRAP, &bootstrap)
        .env(CHILD_LISTEN, &listen)
        .env(CHILD_ADMIN, &admin)
        .env(CHILD_STORE, &store_root)
        .spawn()
        .expect("spawn the querier child");

    // Wait for the WAL tail to be genuinely polling rather than for the
    // process to merely exist: the role exports a poll counter per poll, and a
    // tail that had already failed would export none. Signalling before that
    // would test a tail that never reached its loop.
    wait_for_wal_tail_poll(&runtime, &admin, Duration::from_secs(45));

    // Through `sh` rather than a `kill` binary: the shell builtin is always
    // there, including inside a Bazel test sandbox, and `unsafe_code` is
    // forbidden workspace-wide so `libc::kill` is not an option.
    let signalled = Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -TERM {}", child.id()))
        .status()
        .expect("send SIGTERM");
    assert!(signalled.success());

    let status = wait_for_exit(&mut child, Duration::from_secs(30));

    // `code()` is `None` for a process a signal killed, which is what a role
    // that never returns from its shutdown leaves an orchestrator holding.
    assert!(status.code() == Some(0));
}

/// The role under test: the binary's own `run`, on the real querier arm.
fn run_querier_child(bootstrap: &str) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    // Registered before the parent can see anything this process exports, so
    // the parent's `kill` cannot land in the window before the role installs
    // its own. Tokio's handlers are process-wide and refcounted, so the one
    // the role installs later is this same registration.
    let _terminate = runtime
        .block_on(async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        })
        .expect("install SIGTERM handler");

    let listen = std::env::var(CHILD_LISTEN).expect("child listen address");
    let admin = std::env::var(CHILD_ADMIN).expect("child admin address");
    let store = std::env::var(CHILD_STORE).expect("child block directory");
    let cli = Cli::try_parse_from([
        "krabka-profiles",
        "--target",
        "querier",
        "--listen",
        &listen,
        "--admin-listen-addr",
        &admin,
        "--bootstrap",
        bootstrap,
        "--object-store-url",
        &format!("file://{store}"),
    ])
    .expect("child CLI");

    runtime.block_on(async { run(cli).await.expect("the querier role returns on SIGTERM") });
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

/// An address nothing is listening on yet.
///
/// The child needs its ports named before it starts, because the parent scrapes
/// one of them.
fn free_loopback_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free port");
    listener
        .local_addr()
        .expect("the bound address")
        .to_string()
}

/// Blocks until the child's admin port reports at least one WAL poll.
fn wait_for_wal_tail_poll(runtime: &tokio::runtime::Runtime, admin: &str, within: Duration) {
    let url = format!("http://{admin}/metrics");
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let scraped = runtime.block_on(async {
            match reqwest::get(&url).await {
                Ok(response) => response.text().await.ok(),
                Err(_) => None,
            }
        });
        if scraped.is_some_and(|body| body.contains("krabka_profiles_wal_consumer_polls_total")) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("the querier's WAL tail never polled within {within:?}");
}

fn wait_for_exit(child: &mut std::process::Child, within: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        match child.try_wait().expect("poll the child") {
            Some(status) => return status,
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("the querier did not exit within {within:?} of SIGTERM");
}

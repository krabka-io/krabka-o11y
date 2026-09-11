//! A real `SIGTERM`, sent to a real metrics querier whose broker is
//! unreachable, must make the process exit.
//!
//! An orchestrator stops a container with `SIGTERM` and waits out a grace
//! period before it resorts to `SIGKILL`, so "the handler ran" is not the
//! claim that matters -- "the process exited" is. The querier's supervisor
//! waits for every background task on the way out, and the WAL head consumer
//! opens with a connect that an unreachable broker never finishes: the
//! `krabka-client-consumer` bootstrap retries rather than failing, so without
//! a race against the shutdown token that connect outlives the grace period.
//! The role then reports exit 137, which is a lost drain on every rolling
//! restart of a cluster whose broker is down.
//!
//! `status.code()` is `Some(0)` only for a process that returned from `main`.
//! A process a signal killed reports `None`.

use std::{
    process::Command,
    time::{Duration, Instant},
};

use clap::Parser as _;

use super::{Cli, RoleReadiness, run_querier};

/// Set on the child re-execution of this test binary, and carries the address
/// the child binds its query port on.
const CHILD_LISTEN: &str = "KRABKA_METRICS_TEST_SIGTERM_LISTEN";
const CHILD_ADMIN: &str = "KRABKA_METRICS_TEST_SIGTERM_ADMIN";
const CHILD_BOOTSTRAP: &str = "KRABKA_METRICS_TEST_SIGTERM_BOOTSTRAP";
const CHILD_STORE: &str = "KRABKA_METRICS_TEST_SIGTERM_STORE";

const TEST_NAME: &str = "sigterm_exits_the_querier::sigterm_makes_the_querier_process_exit";

/// A port nothing serves Kafka on. The client's bootstrap retries a refused
/// connection rather than reporting it, so this address stands in for the
/// unreachable broker without needing one.
const DEAD_BROKER: &str = "127.0.0.1:1";

#[test]
fn sigterm_makes_the_querier_process_exit() {
    if std::env::var_os(CHILD_LISTEN).is_some() {
        run_querier_child();
        return;
    }

    let dir = tempfile::tempdir().expect("temporary directory");
    let store_root = dir.path().join("blocks");
    std::fs::create_dir_all(&store_root).expect("block directory");

    let listen = free_loopback_addr();
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_LISTEN, &listen)
        .env(CHILD_ADMIN, free_loopback_addr())
        .env(CHILD_BOOTSTRAP, DEAD_BROKER)
        .env(CHILD_STORE, &store_root)
        .spawn()
        .expect("spawn the querier child");

    // The query port is bound after the shutdown listener is installed and
    // after the WAL head consumer is supervised, so a connection here says the
    // role is past the point the signal has to reach -- and, because the
    // consumer is still connecting, that it is in exactly the state the bug
    // needs.
    wait_for_listener(&listen, Duration::from_secs(45));

    // Through `sh` rather than a `kill` binary: the shell builtin is always
    // there, including inside a Bazel test sandbox, and `unsafe_code` is
    // forbidden workspace-wide so `libc::kill` is not an option.
    let signalled = Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -TERM {}", child.id()))
        .status()
        .expect("send SIGTERM");
    assert2::assert!(signalled.success());

    let status = wait_for_exit(&mut child, Duration::from_secs(30));

    assert2::assert!(status.code() == Some(0));
}

/// The role under test: the binary's own `run_querier`, pointed at a broker
/// that never answers.
fn run_querier_child() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    // Registered before the parent can reach anything this process binds, so
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
    let bootstrap = std::env::var(CHILD_BOOTSTRAP).expect("child bootstrap address");
    let store = std::env::var(CHILD_STORE).expect("child block directory");
    let cli = Cli::try_parse_from([
        "krabka-metrics-service",
        "--target",
        "querier",
        "--listen",
        &listen,
        "--admin-listen-addr",
        &admin,
        "--wal-bootstrap",
        &bootstrap,
        "--object-store-url",
        &format!("file://{store}"),
    ])
    .expect("child CLI");

    runtime.block_on(async {
        run_querier(
            cli,
            krabka_promql::metrics::ServiceMetrics::new(),
            RoleReadiness::new(),
        )
        .await
        .expect("the querier role returns on SIGTERM");
    });
}

/// An address nothing is listening on yet.
fn free_loopback_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free port");
    listener
        .local_addr()
        .expect("the bound address")
        .to_string()
}

fn wait_for_listener(addr: &str, within: Duration) {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the querier never bound {addr} within {within:?}");
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

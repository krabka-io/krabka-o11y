//! A real `SIGTERM`, sent to a real process, must drain the role rather than
//! stop it where it stands.
//!
//! Kubernetes, systemd and `docker stop` all ask a process to stop with
//! `SIGTERM`. A role that does not hear it runs until the grace period expires
//! and is then `SIGKILL`ed: the block it was writing is abandoned and the WAL
//! offset behind it is never committed, so every ordinary rolling restart
//! replays that window. This suite runs the compactor role in a child process,
//! signals it the way an orchestrator would, and asserts on what the drain is
//! supposed to leave behind -- a committed offset, a finalised block, and a
//! server future that returned -- rather than on the signal being received.
#![cfg(unix)]

use std::{
    collections::BTreeMap,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::assert;
use async_trait::async_trait;
use krabka_blockstore::labels;
use krabka_observability::{
    KafkaWalHeader, KafkaWalRecord, LogWalConsumer, Offset, PartitionIndex, QuerierIndexSource,
    Role, ServiceConfig, ServiceDependencies, WalConsumerError, WalLogRecord, WalPosition,
    build_kafka_wal_record, serve_service_listener,
};
use krabka_units::Time;
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};

/// Set on the child re-execution of this test binary, and holds the directory
/// the child and the parent communicate through.
const CHILD_DIR: &str = "KRABKA_TEST_SIGTERM_DRAIN_DIR";

const INDEX_PREFIX: &str = "observability/logs";

#[test]
fn sigterm_drains_the_compactor_before_the_process_exits() {
    if let Some(dir) = std::env::var_os(CHILD_DIR) {
        run_compactor_child(std::path::PathBuf::from(dir));
        return;
    }

    let dir = tempfile::tempdir().expect("temporary directory");
    let root = dir.path().to_path_buf();
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "sigterm_drains_the_compactor_before_the_process_exits",
            "--nocapture",
        ])
        .env(CHILD_DIR, &root)
        .spawn()
        .expect("spawn the compactor child");

    // The child commits only after it has written a block, so this marker also
    // says the role is past its start and has real work to lose.
    let committed = wait_for_file(&root.join("committed"), Duration::from_secs(45));
    assert!(!committed.trim().is_empty());

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

    // Exited of its own accord: `code()` is `None` for a process a signal
    // killed, which is what an unheard SIGTERM leaves behind.
    assert!(status.code() == Some(0));
    // The drain marker is written only after `serve_service_listener` returned,
    // so the role finished its shutdown path instead of merely stopping.
    assert!(root.join("drained").is_file());
    // And the block the compactor was writing reached the store.
    assert!(!block_paths(&root.join("store")).is_empty());
}

/// The role under test: the real compactor service listener, over a real
/// on-disk object store, fed by a consumer that records its commits where the
/// parent can read them.
fn run_compactor_child(root: std::path::PathBuf) {
    // Register the SIGTERM handler before anything the parent can observe, so
    // the parent's `kill` cannot land in the window before the role installs
    // its own. Tokio's handlers are process-wide and refcounted, so the one
    // the role installs later is this same registration.
    let runtime = tokio::runtime::Runtime::new().expect("child runtime");
    let _terminate = runtime
        .block_on(async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        })
        .expect("install SIGTERM handler");

    let store_root = root.join("store");
    std::fs::create_dir_all(&store_root).expect("store directory");
    let store = Arc::new(LocalFileSystem::new_with_prefix(&store_root).expect("object store"));

    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind the compactor listener");
        let config = ServiceConfig {
            target: Role::Compactor,
            listen_addr: "127.0.0.1:0".parse().expect("listen address"),
            object_store_url: None,
            wal_bootstrap_server: None,
            wal_topic: "__krabka_observability_logs_wal".to_string(),
            wal_group_id: "krabka-observability-compactor".to_string(),
            data_root: root.join("data"),
            querier_index_source: QuerierIndexSource::LocalManifest,
            tenant: None,
            index_prefix: Some(INDEX_PREFIX.to_string()),
            ..ServiceConfig::default()
        };
        let dependencies =
            ServiceDependencies::default().with_wal_consumer(CommitLoggingConsumer {
                batches: vec![vec![kafka_wal_record(10, "api ok", 6, 42)]],
                commit_log: root.join("committed"),
            });

        serve_service_listener(listener, config, dependencies, Some(store.as_ref()))
            .await
            .expect("the compactor role returns once it has drained");

        std::fs::write(root.join("drained"), "drained\n").expect("write the drain marker");
    });
}

/// A WAL consumer that appends every committed offset to a file, so the commit
/// survives the process it happened in.
struct CommitLoggingConsumer {
    batches: Vec<Vec<KafkaWalRecord>>,
    commit_log: std::path::PathBuf,
}

#[async_trait]
impl LogWalConsumer for CommitLoggingConsumer {
    async fn poll(&mut self, _timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        if self.batches.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(self.batches.remove(0))
        }
    }

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError> {
        use std::io::Write as _;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.commit_log)
            .expect("open the commit log");
        writeln!(file, "{}:{}", position.partition.0, position.offset.0)
            .expect("record the committed offset");
        Ok(())
    }
}

fn kafka_wal_record(timestamp_ns: i64, line: &str, partition: i32, offset: i64) -> KafkaWalRecord {
    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };
    let producer_record = build_kafka_wal_record("__krabka_observability_logs_wal", &record)
        .expect("producer record");
    KafkaWalRecord {
        value: producer_record.value.expect("producer value").to_vec(),
        partition: PartitionIndex(partition),
        offset: Offset(offset),
        timestamp_ms: producer_record.timestamp_ms,
        headers: producer_record
            .headers
            .into_iter()
            .map(|header| KafkaWalHeader {
                key: header.key,
                value: header.value.map(|value| value.to_vec()),
            })
            .collect(),
    }
}

/// Reads `path` once it exists and is not empty.
///
/// Progress poll rather than a run-duration budget: the deadline only bounds a
/// child that never gets there at all.
fn wait_for_file(path: &std::path::Path, within: Duration) -> String {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Ok(contents) = std::fs::read_to_string(path)
            && !contents.is_empty()
        {
            return contents;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("{} never appeared", path.display());
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
    panic!("the role did not exit within {within:?} of SIGTERM");
}

/// Every block the compactor physically wrote under the index prefix.
fn block_paths(store_root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                found.push(path);
            }
        }
    }

    let mut found = Vec::new();
    walk(
        &store_root.join(ObjectPath::from(INDEX_PREFIX).to_string()),
        &mut found,
    );
    found
}

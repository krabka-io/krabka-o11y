//! The operating-envelope soak: every signal, through its real writer and
//! read path, against one real object store, in eight phases.
//!
//! Each signal runs these phases in order:
//!
//! | Phase | Load |
//! | --- | --- |
//! | `steady` | Two paced writers and two readers on one tenant |
//! | `burst` | Unpaced writers at 1, 2, 4, 8, stopping at the first level that misses an objective |
//! | `high_cardinality` | One paced writer at 100, 1 000, 5 000 series per batch, with the same stop rule |
//! | `cold_blocks` | No writes; every query reloads the index and reads every block the run wrote |
//! | `compaction` | The steady load, with a compaction pass every two seconds |
//! | `deletion` | The steady load, with a retention pass every two seconds |
//! | `noisy_tenant` | The steady load on a quiet tenant beside unpaced writers on a rate-limited noisy one |
//! | `restart` | A cold open over the same store, timed to the first query that sees data |
//!
//! The report, `soak-report.json`, holds one entry per signal and phase:
//! accepted and rejected rates, quantiles, error rate, resident memory,
//! object-store requests and bytes per operation, and recovery time. Its
//! format and the gate that reads it are in `docs/operating_envelope.md`.
//!
//! # What the test asserts
//!
//! Only what a shared runner can promise: every entry is present, the steady
//! phase wrote and read without an error, and a restart recovered. Latency and
//! throughput are measured and reported, and `tools/soak-gate.py` judges them
//! against a baseline recorded on the stable runner.
//!
//! # Running
//!
//! ```text
//! bazel test --config=scale //crates/integration:soak_envelope_docker_test
//! ```
//!
//! Under Cargo, set `KRABKA_MINIO_IMAGE_REF` to the `minio` reference in
//! //bazel/images/images.bzl, and `KRABKA_SOAK_PHASE_SECONDS` to shorten it:
//!
//! ```text
//! KRABKA_MINIO_IMAGE_REF=<ref> KRABKA_SOAK_PHASE_SECONDS=4 \
//!     cargo test -p krabka-integration --test soak_envelope -- --ignored --nocapture
//! ```

mod support;

use std::{sync::Arc, time::Instant};

use assert2::{assert, check};
use serde_json::Value;
use support::{
    SignalKind, config::Config, minio, phases, phases::PHASES, report, store_stats::StoreMeters,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: needs a MinIO container and a wall-clock budget"]
async fn operating_envelope() {
    let config = Arc::new(Config::from_env());
    let (_container, backing) = minio::start().await;
    let started = Instant::now();

    let mut entries = Vec::new();
    for kind in SignalKind::ALL {
        let (stores, meters) = StoreMeters::wrap(&backing, &format!("soak/{}", kind.name()));
        let ctx = phases::context(kind, &stores, meters, &config).await;
        entries.extend(phases::run_signal(kind, &ctx, &stores).await);
    }

    let report = report::build(&config, &entries, started.elapsed().as_secs_f64());
    report::write(&report);
    check_report(&report);
}

fn check_report(report: &Value) {
    let entries = report["entries"]
        .as_array()
        .expect("the report carries entries");
    let entry = |signal: &str, phase: &str| {
        entries
            .iter()
            .find(|e| e["signal"] == signal && e["phase"] == phase)
            .unwrap_or_else(|| panic!("the report has no {signal} {phase} entry"))
    };
    for kind in SignalKind::ALL {
        let signal = kind.name();
        for phase in PHASES {
            let e = entry(signal, phase);
            check!(e["wal_lag"]["status"] == "not_measured", "{signal} {phase}");
        }
        let steady = entry(signal, "steady");
        check!(
            steady["ingest"]["accepted_batches"].as_u64().unwrap_or(0) > 0,
            "{signal} steady wrote nothing: {}",
            steady["error_samples"]
        );
        check!(
            steady["query"]["rows_seen_last"].as_u64().unwrap_or(0) > 0,
            "{signal} steady read nothing back: {}",
            steady["error_samples"]
        );
        check!(
            steady["error_rate"].as_f64() == Some(0.0),
            "{signal} steady errored: {}",
            steady["error_samples"]
        );
        let restart = entry(signal, "restart");
        check!(
            restart["recovery"]["status"] == "recovered",
            "{signal} did not recover: {}",
            restart["error_samples"]
        );
    }
    assert!(entries.len() == SignalKind::ALL.len() * PHASES.len());
}

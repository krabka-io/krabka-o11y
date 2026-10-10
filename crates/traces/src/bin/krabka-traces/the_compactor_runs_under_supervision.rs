//! The compactor role, started and stopped the two ways this binary starts it.
//!
//! Every test here runs the real [`run_compactor`] against an in-memory object
//! store, and reads the compaction run counter to see the loop work. The
//! counter is the only thing outside the role that moves once per pass, so it
//! is also how a loop that outlived the role is caught: a compactor whose loop
//! kept ticking after the role returned would keep raising it.

#[path = "../../../../observability/tests/support/compaction_passes.rs"]
mod compaction_passes;

use std::time::Duration;

use assert2::check;
use clap::Parser as _;
use krabka_observability::{RoleReadiness, StagedDrain};
use krabka_units::secs;
use tokio::sync::oneshot;

use self::compaction_passes::{AFTER_THE_STOP, COMPACTION_INTERVAL, passes, passes_reach};
use super::{CancellationToken, Cli, ServiceMetrics, SharedObjectStore, run_compactor};

fn compactor_cli() -> Cli {
    Cli::try_parse_from([
        "krabka-traces",
        "--target",
        "compactor",
        "--object-store-url",
        "memory:///",
        "--compaction-interval",
        COMPACTION_INTERVAL,
    ])
    .expect("the compactor CLI")
}

/// `--target compactor`: the role returns when its token is cancelled, and the
/// loop is gone when it does.
///
/// The second half is the part supervision buys. A loop that the role merely
/// spawned and forgot would still be ticking here, compacting against a store
/// the process has stopped reporting on, and the role's clean `Ok(())` would
/// say nothing about it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_compactor_returns_and_leaves_no_loop_behind() {
    let metrics = ServiceMetrics::new();
    let readiness = RoleReadiness::new();
    let shutdown = CancellationToken::new();
    let role = tokio::spawn({
        let metrics = metrics.clone();
        let readiness = readiness.clone();
        let shutdown = shutdown.clone();
        async move {
            let object_store = SharedObjectStore::new();
            run_compactor(compactor_cli(), metrics, readiness, shutdown, &object_store).await
        }
    });

    passes_reach(&metrics.compaction, 2).await;
    check!(readiness.is_ready(), "{:?}", readiness.pending());
    shutdown.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(5), role)
        .await
        .expect("the compactor returns when its token is cancelled")
        .expect("the compactor role task");

    check!(outcome.is_ok());
    let stopped_at = passes(&metrics.compaction);
    tokio::time::sleep(AFTER_THE_STOP).await;
    check!(
        passes(&metrics.compaction) == stopped_at,
        "a compaction loop was still running after the role returned"
    );
}

/// A role started with its token already cancelled stops without a pass, and
/// without reporting the stop as a fault.
///
/// This is the shutdown that arrives during startup. The supervisor treats a
/// loop that ends after the token is cancelled as the stop working, so the role
/// has to return `Ok(())` here rather than a [`CriticalTaskError`] naming its
/// own loop.
///
/// [`CriticalTaskError`]: krabka_observability::CriticalTaskError
#[tokio::test]
async fn a_compactor_started_during_shutdown_returns_without_a_pass() {
    let metrics = ServiceMetrics::new();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let object_store = SharedObjectStore::new();
    let outcome = run_compactor(
        compactor_cli(),
        metrics.clone(),
        RoleReadiness::new(),
        shutdown,
        &object_store,
    )
    .await;

    check!(outcome.is_ok());
    check!(passes(&metrics.compaction) == 0);
}

/// `--target all`: the compactor stage stops inside the drain's budget.
///
/// The composition stops one stage at a time and waits for each, so a stage
/// that returned while its loop kept running, or that did not return at all,
/// would hold the whole stop open or outlive it. `StagedDrain` reports either
/// as an overrun, and the run counter shows whether the loop really went with
/// the stage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_compactor_stage_of_target_all_stops_within_its_drain_budget() {
    let metrics = ServiceMetrics::new();
    let (outcome_tx, outcome_rx) = oneshot::channel();
    let mut drain = StagedDrain::new(secs(5));
    let staged = metrics.clone();
    drain.stage("compactor", move |token| async move {
        let object_store = SharedObjectStore::new();
        let outcome = run_compactor(
            compactor_cli(),
            staged,
            RoleReadiness::new(),
            token,
            &object_store,
        )
        .await;
        let _ = outcome_tx.send(outcome.is_ok());
    });

    passes_reach(&metrics.compaction, 2).await;
    let overran = drain.drain().await;

    check!(overran.is_empty(), "the compactor stage overran its budget");
    check!(
        outcome_rx.await.expect("the stage reports its outcome"),
        "the compactor stage returned an error"
    );
    let stopped_at = passes(&metrics.compaction);
    tokio::time::sleep(AFTER_THE_STOP).await;
    check!(
        passes(&metrics.compaction) == stopped_at,
        "a compaction loop outlived the stage that owned it"
    );
}

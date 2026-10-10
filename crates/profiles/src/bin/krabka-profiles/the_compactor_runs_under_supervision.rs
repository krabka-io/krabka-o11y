//! The compactor role, started and stopped the two ways this binary starts it.
//!
//! Every test here runs the real role against an in-memory object store, and
//! reads the compaction run counter to see the loop work. The counter is the
//! only thing outside the role that moves once per pass, so it is also how a
//! loop that outlived the role is caught: a compactor whose loop kept ticking
//! after the role returned would keep raising it.

#[path = "../../../../observability/tests/support/compaction_passes.rs"]
mod compaction_passes;

use std::{sync::Arc, time::Duration};

use assert2::check;
use clap::Parser as _;
use krabka_observability::RoleReadiness;
use krabka_profiles::limits::{Limits, OverridesProvider};
use object_store::memory::InMemory;
use tokio::sync::oneshot;

use self::compaction_passes::{
    COMPACTION_INTERVAL, assert_no_pass_after_the_stop, assert_stage_drains_with_its_loop,
    assert_stopped_without_a_pass, passes_reach,
};
use super::{CancellationToken, Cli, ObjectStore, ServiceMetrics, compactor_stage, run_compactor};

fn compactor_cli() -> Cli {
    Cli::try_parse_from([
        "krabka-profiles",
        "--target",
        "compactor",
        "--object-store-url",
        "memory:///",
        "--compactor-interval",
        COMPACTION_INTERVAL,
    ])
    .expect("the compactor CLI")
}

/// `--target compactor`: the role returns when its token is cancelled, and the
/// loop is gone when it does.
///
/// The second half is the part supervision buys. A loop that the role merely
/// spawned and forgot would still be ticking here, compacting and deleting
/// against a store the process has stopped reporting on, and the role's clean
/// `Ok(())` would say nothing about it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_compactor_returns_and_leaves_no_loop_behind() {
    let metrics = ServiceMetrics::new();
    let readiness = RoleReadiness::new();
    let shutdown = CancellationToken::new();
    // The role is awaited here rather than spawned: its error type is a bare
    // `Box<dyn Error>`, so its future is not `Send`. The cancel therefore
    // arrives from a watcher task once the role is demonstrably running.
    let watcher = tokio::spawn({
        let metrics = metrics.clone();
        let shutdown = shutdown.clone();
        async move {
            passes_reach(&metrics.compaction, 2).await;
            shutdown.cancel();
        }
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        run_compactor(
            compactor_cli(),
            metrics.clone(),
            readiness.clone(),
            shutdown,
        ),
    )
    .await
    .expect("the compactor returns when its token is cancelled");
    watcher.await.expect("the watcher task");

    check!(outcome.is_ok());
    check!(readiness.is_ready(), "{:?}", readiness.pending());
    assert_no_pass_after_the_stop(
        &metrics.compaction,
        "a compaction loop was still running after the role returned",
    )
    .await;
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

    let outcome = run_compactor(
        compactor_cli(),
        metrics.clone(),
        RoleReadiness::new(),
        shutdown,
    )
    .await;

    assert_stopped_without_a_pass(&outcome, &metrics.compaction);
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
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stage = compactor_stage(
        &Arc::new(compactor_cli()),
        store,
        "index/profiles.json".to_string(),
        OverridesProvider::new(Limits::default()),
        &metrics,
    );
    let (stopped_tx, stopped_rx) = oneshot::channel();
    assert_stage_drains_with_its_loop(&metrics.compaction, move |token| async move {
        stage(token).await;
        let _ = stopped_tx.send(());
    })
    .await;

    check!(stopped_rx.await.is_ok(), "the compactor stage returned");
}

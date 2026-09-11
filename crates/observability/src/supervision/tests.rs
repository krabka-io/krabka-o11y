use assert2::assert;
use tokio_util::sync::CancellationToken;

use super::{CriticalTaskError, StagedDrain, SupervisedTasks};

/// A supervised task that panics must be reported, by name, as a critical
/// exit -- the case a dropped `JoinHandle` loses entirely.
#[tokio::test]
async fn a_panicking_task_resolves_as_an_unexpected_exit() {
    let token = CancellationToken::new();
    let mut tasks = SupervisedTasks::new(token.clone());
    tasks.spawn("wal consumer", async { panic!("one bad record") });

    let name = tasks.first_unexpected_exit().await;

    assert!(name == "wal consumer");
    assert!(token.is_cancelled());
}

/// A loop that returns early is the same fault seen from outside: the role
/// keeps its listener and nothing advances.
#[tokio::test]
async fn a_task_that_returns_early_resolves_as_an_unexpected_exit() {
    let token = CancellationToken::new();
    let mut tasks = SupervisedTasks::new(token.clone());
    tasks.spawn("index refresher", async {});

    let name = tasks.first_unexpected_exit().await;

    assert!(name == "index refresher");
    assert!(token.is_cancelled());
}

/// Tasks stopping because the role is shutting down are the shutdown working.
/// Reporting them would turn every clean stop into a spurious failure.
#[tokio::test]
async fn exits_after_cancellation_are_not_reported() {
    let token = CancellationToken::new();
    let mut tasks = SupervisedTasks::new(token.clone());
    let task_token = token.clone();
    tasks.spawn("wal consumer", async move { task_token.cancelled().await });
    token.cancel();

    let reported = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        tasks.first_unexpected_exit(),
    )
    .await;

    assert!(reported.is_err());
}

/// A role may register nothing at all. Selecting its main future against an
/// empty supervisor must not resolve instantly and end the role.
#[tokio::test]
async fn an_empty_supervisor_never_resolves() {
    let mut tasks = SupervisedTasks::new(CancellationToken::new());
    assert!(tasks.is_empty());

    let reported = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        tasks.first_unexpected_exit(),
    )
    .await;

    assert!(reported.is_err());
}

/// Shutdown cancels the token and waits, so a graceful stop drains rather
/// than detaching what was still running.
#[tokio::test]
async fn shutdown_cancels_and_drains() {
    let token = CancellationToken::new();
    let mut tasks = SupervisedTasks::new(token.clone());
    let task_token = token.clone();
    tasks.spawn("wal consumer", async move { task_token.cancelled().await });

    tasks.shutdown().await;

    assert!(token.is_cancelled());
}

#[test]
fn the_error_names_the_task() {
    assert!(
        CriticalTaskError("querier WAL hot-tail").to_string()
            == "critical background task `querier WAL hot-tail` stopped unexpectedly"
    );
}

/// The order is the whole point of a staged drain. A distributor asked to stop
/// after the block builder keeps writing records into a WAL nothing is
/// reading, and in a one-process stack nothing restarts to pick them up.
#[tokio::test]
async fn stages_are_stopped_in_registration_order_and_waited_for() {
    let stopped: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut drain = StagedDrain::new(krabka_units::secs(5));
    for name in ["distributor", "block-builder", "querier"] {
        let stopped = std::sync::Arc::clone(&stopped);
        drain.stage(name, move |token| async move {
            token.cancelled().await;
            // A stage that takes its time is the case worth holding: the next
            // stage must not be asked to stop until this one has finished.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            stopped.lock().expect("recording lock").push(name);
        });
    }

    assert!(drain.order() == ["distributor", "block-builder", "querier"]);
    let overran = drain.drain().await;

    assert!(overran.is_empty());
    assert!(
        *stopped.lock().expect("recording lock") == ["distributor", "block-builder", "querier"]
    );
}

/// A role that stops on its own while the process is serving leaves the port
/// open over a stage that is gone. It has to be reported by name.
#[tokio::test]
async fn a_stage_that_stops_on_its_own_is_reported() {
    let mut drain = StagedDrain::new(krabka_units::secs(5));
    drain.stage("block-builder", |_token| async { panic!("one bad record") });
    drain.stage("querier", |token| async move { token.cancelled().await });

    let name = drain.first_unexpected_exit().await;

    assert!(name == "block-builder");
}

/// Every stage stops during a drain, and reporting those as faults would turn
/// each clean stop into a spurious non-zero exit.
#[tokio::test]
async fn exits_during_a_drain_are_not_reported_as_faults() {
    let mut drain = StagedDrain::new(krabka_units::secs(5));
    drain.stage(
        "distributor",
        |token| async move { token.cancelled().await },
    );
    let overran = drain.drain().await;

    assert!(overran.is_empty());
}

/// A stage that ignores its token must not hold the stop open past an
/// orchestrator's grace period. It is named and left behind.
#[tokio::test]
async fn a_stage_that_will_not_stop_is_named_and_left_behind() {
    let mut drain = StagedDrain::new(krabka_units::millis(50));
    drain.stage("block-builder", |_token| std::future::pending());
    drain.stage("querier", |token| async move { token.cancelled().await });

    let overran = drain.drain().await;

    assert!(overran == ["block-builder"]);
}

/// A process with no stages registered must still be safe to select against.
#[tokio::test]
async fn an_empty_drain_never_reports_an_exit() {
    let mut drain = StagedDrain::new(krabka_units::secs(5));

    let reported = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        drain.first_unexpected_exit(),
    )
    .await;

    assert!(reported.is_err());
}

use assert2::assert;
use tokio_util::sync::CancellationToken;

use super::{CriticalTaskError, SupervisedTasks};

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

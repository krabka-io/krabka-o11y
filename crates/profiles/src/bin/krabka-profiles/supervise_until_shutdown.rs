use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::CancellationToken;

/// Waits until `shutdown` fires or one of `tasks` exits unexpectedly, then
/// shuts every task down.
///
/// # Errors
/// Returns a [`CriticalTaskError`] naming the first task that exited before
/// `shutdown` fired.
pub(crate) async fn supervise_until_shutdown(
    mut tasks: SupervisedTasks,
    shutdown: &CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        name = tasks.first_unexpected_exit() => {
            Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
        }
    };
    // A task already under way finishes before this returns, so `--target
    // all` stops this stage and moves to the next one rather than leaving a
    // loop running behind the drain.
    tasks.shutdown().await;
    outcome
}

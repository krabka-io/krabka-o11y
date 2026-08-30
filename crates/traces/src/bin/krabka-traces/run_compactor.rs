use super::{CancellationToken, Cli, run_compactor_once};

pub(crate) async fn run_compactor(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => Ok(()),
        result = run_compactor_once(cli) => result,
    }
}

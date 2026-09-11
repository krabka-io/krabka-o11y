use krabka_observability::SupervisedTasks;

use super::{AllStage, Arc, Cli, ClientSecurity, ProfileReadPath, ServiceMetrics};

/// The read path's shared background work, as `--target all` runs it.
///
/// The WAL tail and the index refresher feed both read roles, because both
/// answer from one [`ProfileReadPath`]. They are staged with the
/// query-frontend rather than with the querier because the frontend is the
/// read role that outlives the other: stopping them with the querier would
/// leave the frontend answering from a hot window and an index that had
/// stopped moving, which reads as missing data rather than as a role that is
/// shutting down.
///
/// Either loop ending is the end of the stage. Both failures are silent in the
/// answer -- an index that stopped refreshing hides every block written since,
/// a WAL tail that stopped hides the last few minutes -- so the process has to
/// hear about them from somewhere, and this is where.
pub(crate) fn read_path_stage(
    cli: &Arc<Cli>,
    read: ProfileReadPath,
    metrics: &ServiceMetrics,
    wal_security: Option<ClientSecurity>,
) -> AllStage {
    let cli = Arc::clone(cli);
    let metrics = metrics.clone();
    Box::new(move |token| {
        Box::pin(async move {
            let mut tasks = SupervisedTasks::new(token.clone());
            for (name, handle) in
                read.spawn_background(&cli, &metrics, &token, wal_security.as_ref())
            {
                tasks.adopt(name, handle);
            }
            tokio::select! {
                () = token.cancelled() => {}
                name = tasks.first_unexpected_exit() => {
                    tracing::error!(task = name, "profiles read path lost a task it cannot answer without");
                }
            }
            tasks.shutdown().await;
        })
    })
}

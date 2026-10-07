use krabka_observability::CancellationToken;

use super::{
    Cli, ClientSecurity, RoleReadiness, ServerSecurity, ServiceMetrics, Target,
    require_role_topics, run_block_builder, run_compactor, run_distributor,
};

/// Runs one writer role with the caller's readiness and shutdown controls.
///
/// Registers its startup gate before the returned future is polled.
/// The caller loads security before it binds any process listener. It owns
/// the admin port and the audit writer. Cancellation drains the role before
/// this function returns.
///
/// # Errors
/// Returns an error when the topic contract fails, startup fails, or a
/// required background task stops unexpectedly.
pub fn serve(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    security: ServerSecurity,
    wal_security: Option<ClientSecurity>,
    stopping: CancellationToken,
) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static {
    let startup = readiness.gate("startup");
    Box::pin(async move {
        require_role_topics(&cli, wal_security.clone()).await?;
        readiness.track_wal_consumer(metrics.wal_consumer.clone());
        readiness.track_object_store(metrics.object_store.clone());
        match cli.target {
            Target::Distributor => {
                run_distributor(
                    cli,
                    metrics,
                    readiness,
                    &security,
                    wal_security,
                    stopping,
                    startup,
                )
                .await
            }
            Target::BlockBuilder => {
                Box::pin(run_block_builder(
                    cli,
                    metrics,
                    readiness,
                    wal_security,
                    stopping,
                    startup,
                ))
                .await
            }
            Target::Compactor => run_compactor(cli, metrics, readiness, stopping, startup).await,
        }
    })
}

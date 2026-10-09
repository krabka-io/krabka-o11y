use krabka_observability::CancellationToken;

use super::{
    Cli, ClientSecurity, RoleReadiness, ServerSecurity, ServiceMetrics, Target, WriterConfig,
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
    serve_writer(
        (cli.target, cli.writer),
        metrics,
        readiness,
        security,
        wal_security,
        stopping,
    )
}

/// Runs an embedded writer role with its caller's process controls.
/// Registers startup before the returned future is polled.
/// # Errors
/// Returns a topic, startup, I/O or supervision error.
pub fn serve_writer(
    (target, cli): (Target, WriterConfig),
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    security: ServerSecurity,
    wal_security: Option<ClientSecurity>,
    stopping: CancellationToken,
) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static {
    let startup = readiness.gate("startup");
    // Register sources in role order before tasks start. The shared recovery
    // report can then identify the durable writer even before its first commit.
    if target == Target::BlockBuilder {
        readiness.track_wal_consumer(metrics.wal_consumer.clone());
    }
    readiness.track_object_store(metrics.object_store.clone());
    Box::pin(async move {
        require_role_topics(&cli, target, wal_security.clone()).await?;
        match target {
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

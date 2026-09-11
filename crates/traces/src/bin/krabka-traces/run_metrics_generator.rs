use krabka_observability::RoleReadiness;

use super::{
    Arc, CancellationToken, Cli, KafkaSpanSource, MetricsGenConfig, MetricsGenService,
    ProcessSecurity, PrometheusRemoteWriteSink, SystemClock, apply_metrics_generator_cli_overrides,
    wal_consumer,
};

pub(crate) async fn run_metrics_generator(
    cli: Cli,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    security: &ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The spans this role reads all come from the WAL. Until the consumer has
    // a broker it generates no metrics at all, and the admin port says so.
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let mut cfg = if let Some(path) = &cli.config {
        let bytes = std::fs::read_to_string(path)?;
        serde_yaml::from_str::<MetricsGenConfig>(&bytes)?
    } else {
        MetricsGenConfig::default()
    };
    apply_metrics_generator_cli_overrides(&mut cfg, &cli);

    let consumer = wal_consumer(
        &cli,
        "krabka-traces-metrics-generator",
        None,
        security.wal.as_ref(),
    )
    .await?;
    wal_consumer_gate.mark_ready();
    let source = Arc::new(KafkaSpanSource::new(consumer));
    // The remote-write target is a Krabka metrics distributor, so the sink
    // presents the internal client credential.
    let sink = Arc::new(PrometheusRemoteWriteSink::new(
        cfg.remote_write_url.clone(),
        security.server.internal_client(),
    )?);
    let service = MetricsGenService::new(cfg, Arc::new(SystemClock), source, sink)
        .with_poll_policy(
            cli.metrics_generator_poll_batch_size,
            cli.metrics_generator_poll_error_backoff,
        );
    service.run(shutdown).await?;
    Ok(())
}

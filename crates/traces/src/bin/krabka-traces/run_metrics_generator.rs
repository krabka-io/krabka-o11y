use super::{Arc, CancellationToken, Cli, KafkaSpanSource, MetricsGenConfig, MetricsGenService, PrometheusRemoteWriteSink, SystemClock, apply_metrics_generator_cli_overrides, wal_consumer};

pub(crate) async fn run_metrics_generator(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut cfg = if let Some(path) = &cli.config {
        let bytes = std::fs::read_to_string(path)?;
        serde_yaml::from_str::<MetricsGenConfig>(&bytes)?
    } else {
        MetricsGenConfig::default()
    };
    apply_metrics_generator_cli_overrides(&mut cfg, &cli);

    let consumer = wal_consumer(
        cli.bootstrap,
        "krabka-traces-metrics-generator",
        None,
        cli.wal_fetch_max,
        cli.wal_fetch_partition_max,
        cli.client_dispatch_queue_capacity,
        cli.client_frame_max,
    )
    .await?;
    let source = Arc::new(KafkaSpanSource::new(consumer));
    let sink = Arc::new(PrometheusRemoteWriteSink::new(cfg.remote_write_url.clone()));
    let service = MetricsGenService::new(cfg, Arc::new(SystemClock), source, sink)
        .with_poll_policy(
            cli.metrics_generator_poll_batch_size,
            cli.metrics_generator_poll_error_backoff,
        );
    service.run(shutdown).await?;
    Ok(())
}

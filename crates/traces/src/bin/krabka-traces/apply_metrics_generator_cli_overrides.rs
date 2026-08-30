use super::*;

pub(crate) fn apply_metrics_generator_cli_overrides(cfg: &mut MetricsGenConfig, cli: &Cli) {
    if let Some(interval) = cli.collection_interval {
        cfg.collection_interval = interval;
    }
    if let Some(url) = &cli.remote_write_url {
        cfg.remote_write_url.clone_from(url);
    }
    if let Some(max) = cli.max_exemplars_per_series {
        cfg.max_exemplars_per_series = max;
    }
    if let Some(ttl) = cli.edge_ttl {
        cfg.edge_ttl = ttl;
    }
    if let Some(max) = cli.edge_store_max_items {
        cfg.edge_store_max_items = max;
    }
    if let Some(buckets) = &cli.histogram_buckets {
        cfg.histogram_buckets_ns = buckets
            .iter()
            .map(|bucket| bucket.secs_f64() * 1_000_000_000.0)
            .collect();
    }
    cfg.enable_target_info |= cli.metrics.enable_target_info;
    cfg.enable_status_message |= cli.metrics.enable_status_message;
    cfg.enable_messaging_system_latency |= cli.metrics.enable_messaging_system_latency;
}

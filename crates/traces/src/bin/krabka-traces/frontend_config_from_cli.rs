use super::*;

/// Map the role CLI onto the new query-frontend [`FrontendConfig`].
///
/// `--querier-url` is a comma-separated list of querier URLs that carry a
/// scheme. The new [`HttpQuerier`] pool takes a bare `host:port`, so this
/// function strips the scheme and the path.
///
/// `--live-frontier`, and its legacy `--live-frontier-ns` alias, maps to
/// `hot_frontier_ns`. `None` becomes `0`, so the live tier is always probed.
pub(crate) fn frontend_config_from_cli(
    cli: &Cli,
    listen_addr: SocketAddr,
) -> Result<FrontendConfig, Box<dyn std::error::Error + Send + Sync>> {
    let querier_addrs = parse_querier_addrs(&cli.querier_url)?;
    Ok(FrontendConfig {
        querier_addrs,
        target_per_job: cli.target_bytes_per_job,
        max_concurrency: cli.query_queue_depth.max(1),
        hot_frontier_ns: cli.live_frontier.unwrap_or(UnixNano(0)).0,
        max_trace: max_trace_size(cli.max_trace_spans),
        listen_addr,
        ..FrontendConfig::default()
    })
}

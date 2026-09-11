use super::{Cli, FrontendConfig, SocketAddr, UnixNano, max_trace_size, parse_querier_addrs};

/// Map the role CLI onto the new query-frontend [`FrontendConfig`].
///
/// `--querier-url` is a comma-separated list of querier URLs that carry a
/// scheme. The membership refresh resolves and probes bare `host:port`, so
/// this function strips the path and keeps the one scheme of the list for the
/// transport.
///
/// `--live-frontier`, and its legacy `--live-frontier-ns` alias, maps to
/// `hot_frontier_ns`. `None` becomes `0`, so the live tier is always probed.
pub(crate) fn frontend_config_from_cli(
    cli: &Cli,
    listen_addr: SocketAddr,
) -> Result<FrontendConfig, Box<dyn std::error::Error + Send + Sync>> {
    let (querier_scheme, querier_addrs) = parse_querier_addrs(&cli.querier_url)?;
    Ok(FrontendConfig {
        querier_addrs,
        querier_scheme,
        target_per_job: cli.target_bytes_per_job,
        max_concurrency: cli.query_queue_depth.max(1),
        hot_frontier_ns: cli.live_frontier.unwrap_or(UnixNano(0)).0,
        max_trace: max_trace_size(cli.max_trace_spans),
        membership_refresh_interval: cli.querier_membership_refresh_interval,
        readiness_timeout: cli.querier_readiness_timeout,
        listen_addr,
        ..FrontendConfig::default()
    })
}

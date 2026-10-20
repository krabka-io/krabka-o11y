use krabka_promql::MetricStore;

use super::{Arc, Cli, PrometheusApiState, query_engine_opts};

/// The Prometheus API state a serving role starts from: the engine, the
/// concurrency, timeout and remote-read limits the CLI configures, and the
/// process log level in the runtime status.
pub(crate) fn serving_api_state<S: MetricStore>(
    metric_store: Arc<S>,
    cli: &Cli,
) -> PrometheusApiState<S> {
    PrometheusApiState::new(metric_store, query_engine_opts(cli))
        .with_max_concurrent_queries(cli.max_concurrent_queries)
        .with_query_timeout(cli.query_timeout)
        .with_remote_read_max_body(cli.remote_read_max_body)
        .with_runtime_status(
            krabka_observability::LogLevelControl::process().level(),
            None,
        )
}

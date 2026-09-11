use super::{Cli, EngineOpts};

pub(crate) fn query_engine_opts(cli: &Cli) -> EngineOpts {
    EngineOpts {
        lookback_delta: cli.query_lookback_delta,
        eval_interval: cli.query_eval_interval,
        max_samples: cli.query_max_samples,
        // Each query's series cap is its tenant's `max_fetched_series_per_query`,
        // which `PrometheusApiState::engine_for_tenant` sets.
        max_fetched_series: 0,
    }
}

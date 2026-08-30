use super::{Cli, EngineOpts};

pub(crate) fn query_engine_opts(cli: &Cli) -> EngineOpts {
    EngineOpts {
        lookback_delta: cli.query_lookback_delta,
        eval_interval: cli.query_eval_interval,
        max_samples: cli.query_max_samples,
    }
}

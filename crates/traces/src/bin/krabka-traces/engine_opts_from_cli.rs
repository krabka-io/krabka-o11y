use super::{Cli, EngineOpts};

pub(crate) fn engine_opts_from_cli(cli: &Cli) -> std::io::Result<EngineOpts> {
    if !cli
        .traceql_histogram_buckets
        .windows(2)
        .all(|pair| pair[0] < pair[1])
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "TraceQL histogram buckets must be strictly increasing",
        ));
    }
    Ok(EngineOpts {
        default_limit: cli.traceql_default_limit,
        default_spss: cli.traceql_default_spss,
        max_traces: cli.max_search_traces,
        max_exemplars: cli.max_metric_exemplars,
        compare_max_values_per_attr: cli.traceql_compare_max_values_per_attr,
        histogram_buckets: cli.traceql_histogram_buckets.clone(),
    })
}

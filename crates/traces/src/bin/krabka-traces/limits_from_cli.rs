use super::{Cli, Limits, ingest_rate_from_cli, u64_limit_from_usize};

/// Build the process-wide limits from the command line.
///
/// These become the defaults of the one [`OverridesProvider`] each role
/// constructs, so a flag typed here reaches every tenant that the runtime
/// overrides file does not name, and every gate that resolves a tenant.
///
/// [`OverridesProvider`]: krabka_traces::limits::OverridesProvider
pub(crate) fn limits_from_cli(cli: &Cli) -> Limits {
    Limits {
        ingestion_rate: ingest_rate_from_cli(cli.max_ingest_spans_per_second),
        ingestion_burst_spans: u64_limit_from_usize(cli.ingest_rate_burst),
        max_spans_per_request: u64_limit_from_usize(cli.max_spans_per_request),
        max_traces_per_search: u64_limit_from_usize(cli.max_traces_per_search),
        max_spans_per_trace: u64_limit_from_usize(cli.max_spans_per_trace),
        max_attribute: cli.max_attr_value_len,
        max_search_duration: cli.max_search_duration,
        block_retention: cli.block_retention,
    }
}

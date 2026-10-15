use super::*;
use std::path::Path;

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
    if let Some(max) = cli.max_active_series {
        cfg.max_active_series = max;
    }
    if let Some(max) = cli.metrics_generator_max_tenants {
        cfg.max_tenants = max;
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

/// Reads the compaction policy the operator configured.
///
/// The unit is not configurable: a traces block counts its timestamps in epoch
/// nanoseconds, so that is the unit the policy converts its window into.
pub(crate) fn compaction_policy_from_cli(cli: &Cli) -> CompactionPolicy {
    CompactionPolicy::new(
        cli.compaction_max_blocks_per_job,
        cli.compaction_target_rows,
        BlockLevel(cli.compaction_max_level),
        cli.compaction_level_window,
        BlockTimestampUnit::Nanos,
    )
}

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

pub(crate) fn f64_from_usize(value: usize) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

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
    let queriers = parse_querier_addrs(&cli.querier_url)?;
    Ok(FrontendConfig {
        querier_addrs: queriers.addresses,
        querier_scheme: queriers.scheme,
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

/// `usize::MAX` is the CLI's "no limit" spelling. A zero rate is how the shared
/// limits express unlimited.
pub(crate) fn ingest_rate_from_cli(spans_per_sec: usize) -> Frequency {
    if spans_per_sec == usize::MAX {
        <Frequency as FrequencyExt>::ZERO
    } else {
        Frequency::from_per_sec(f64_from_usize(spans_per_sec))
    }
}

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
        ..Limits::default()
    }
}

pub(crate) fn live_i64_param(uri: &axum::http::Uri, name: &str) -> Result<i64, String> {
    uri.query()
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|entry| entry.0 == name)
                .map(|entry| entry.1.into_owned())
        })
        .ok_or_else(|| format!("missing query parameter {name}"))?
        .parse::<i64>()
        .map_err(|_| format!("invalid query parameter {name}"))
}

/// Read the per-tenant limit overrides this process was pointed at.
///
/// `defaults` are the limits the command line built. A tenant the file does not
/// name gets them unchanged, and a tenant it does name gets them with only the
/// keys of its entry replaced. With no file, every tenant gets `defaults`, so a
/// caller always has a provider and never a `None` to fall back from.
///
/// # Errors
/// Returns an error when the file cannot be read, or when it is not the
/// expected YAML document.
pub(crate) fn load_traces_limits_overrides_config(
    path: Option<&Path>,
    api_path: Option<&Path>,
    defaults: Limits,
) -> Result<OverridesProvider, Box<dyn std::error::Error + Send + Sync>> {
    let provider = if let Some(path) = path {
        let text = std::fs::read_to_string(path)?;
        OverridesProvider::from_yaml_with_defaults(&text, defaults)?
    } else {
        OverridesProvider::new(defaults)
    };
    match api_path {
        Some(path) => provider
            .with_api_file(path.to_path_buf())
            .map_err(Into::into),
        None => Ok(provider),
    }
}

/// The assembled-trace ceiling. It budgets about 64KiB of OTLP JSON per span.
pub(crate) fn max_trace_size(spans: usize) -> ByteSize {
    kibibytes(64) * f64_from_usize(spans)
}

pub(crate) fn parse_non_negative_time_or_secs(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_secs, false)
}

pub(crate) fn parse_non_negative_whole_byte_size_or_bytes(value: &str) -> Result<ByteSize, String> {
    let size = value.parse::<u64>().map_or_else(
        |_| parse::non_negative_byte_size(value).map_err(|error| error.to_string()),
        |bytes| Ok(ByteSize::from_bytes(bytes)),
    )?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
        return Err(
            "size must be a non-negative whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    Ok(size)
}

pub(crate) fn parse_positive_time_or_millis(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_millis, true)
}

pub(crate) fn parse_positive_time_or_nanos(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_nanos, true)
}

pub(crate) fn parse_positive_time_or_nanos_f64(value: &str) -> Result<Time, String> {
    value.parse::<f64>().map_or_else(
        |_| parse::positive_time(value).map_err(|error| error.to_string()),
        |value| {
            if value.is_finite() && value > 0.0 {
                Ok(Time::from_secs_f64(value / 1_000_000_000.0))
            } else {
                Err("time must be finite and positive".to_owned())
            }
        },
    )
}

pub(crate) fn parse_positive_time_or_secs(value: &str) -> Result<Time, String> {
    parse_time_or_legacy_i64(value, Time::from_secs, true)
}

pub(crate) fn parse_scan_concat_max(value: &str) -> Result<ByteSize, String> {
    let size = parse_positive_whole_byte_size(value)?;
    if size > DEFAULT_SCAN_CONCAT_MAX {
        return Err("scan concatenation maximum must not exceed 1.5GB".to_owned());
    }
    Ok(size)
}

pub(crate) fn parse_time_or_legacy_i64(
    value: &str,
    legacy_unit: fn(i64) -> Time,
    positive: bool,
) -> Result<Time, String> {
    if let Ok(value) = value.parse::<i64>() {
        if value < 0 || (positive && value == 0) {
            return Err("time must be positive".to_owned());
        }
        return Ok(legacy_unit(value));
    }
    if positive {
        parse::positive_time(value)
    } else {
        parse::non_negative_time(value)
    }
    .map_err(|error| error.to_string())
}

pub(crate) fn parse_unix_nano(value: &str) -> Result<UnixNano, String> {
    if value == "max" {
        return Ok(UnixNano(i64::MAX));
    }
    if let Ok(value) = value.parse::<i64>() {
        return Ok(UnixNano(value));
    }
    parse::time(value)
        .map(|value| UnixNano(value.nanos_i64()))
        .map_err(|error| error.to_string())
}

pub(crate) fn promoted_attrs_from_cli(cli: &Cli) -> Result<Vec<PromotedSpanAttr>, String> {
    let mut attrs = Vec::new();
    for spec in &cli.promote_resource_attrs {
        attrs.push(parse_promoted_attr(spec, Some(RESOURCE_ATTR_PREFIX))?);
    }
    for spec in &cli.promote_span_attrs {
        attrs.push(parse_promoted_attr(spec, None)?);
    }
    Ok(attrs)
}

/// Convert a `usize` command-line limit into the `u64` the shared limits hold.
///
/// `usize::MAX` is the command line's spelling of "no limit", and zero is what
/// [`krabka_traces::Limits`] reads as unlimited, so the sentinel maps to zero.
/// Every other value maps to itself.
pub(crate) fn u64_limit_from_usize(value: usize) -> u64 {
    if value == usize::MAX {
        0
    } else {
        u64::try_from(value).unwrap_or(u64::MAX)
    }
}

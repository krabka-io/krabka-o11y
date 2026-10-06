use super::{Limits, ServiceConfig};

/// The process defaults the scalar CLI flags describe.
///
/// Each flag is optional, and an unset flag keeps `Loki`'s default. The two
/// timestamp-window flags always have a value, because `clap` gives them
/// `Loki`'s own default.
pub(crate) fn limits_for_config(config: &ServiceConfig) -> Limits {
    let defaults = Limits::default();
    Limits {
        reject_old_samples_max_age: config.reject_old_samples_max_age,
        creation_grace_period: config.creation_grace_period,
        max_ingest_body: config.max_ingest_body.unwrap_or(defaults.max_ingest_body),
        max_query_range: config.max_query_range.unwrap_or(defaults.max_query_range),
        max_query_series: config
            .max_query_series
            .map_or(defaults.max_query_series, |series| {
                u64::try_from(series).unwrap_or(u64::MAX)
            }),
        discover_log_levels: config.discover_log_levels,
        log_level_fields: config.log_level_fields.clone(),
        log_level_from_json_max_depth: config.log_level_from_json_max_depth,
        enable_multi_variant_queries: config.enable_multi_variant_queries,
        shard_aggregations: config.shard_aggregations.clone(),
        max_query_read: config.max_query_read.unwrap_or(defaults.max_query_read),
        max_query_string_bytes: config
            .max_query_string_bytes
            .unwrap_or(defaults.max_query_string_bytes),
        retention_period: config.retention_period.unwrap_or(defaults.retention_period),
        ..defaults
    }
}

use std::num::NonZeroUsize;

use assert2::assert;
use clap::Parser;
use krabka_observability::{
    ServiceConfig, audit::AuditArgs, server_security::ServerSecurityArgs,
    wal_client_security::WalClientSecurityArgs,
};
use krabka_units::{days, hours, kibibytes, mebibytes, millis, minutes, secs};

#[path = "support/scoped_querier_config.rs"]
mod scoped_querier_config;

use scoped_querier_config::scoped_querier_config;

struct EnvironmentOverride {
    name: &'static str,
    value: &'static str,
}

fn environment_override(name: &'static str, value: &'static str) -> EnvironmentOverride {
    EnvironmentOverride { name, value }
}

fn with_environment<T>(
    overrides: impl IntoIterator<Item = EnvironmentOverride>,
    run: impl FnOnce() -> T,
) -> T {
    let overrides = overrides.into_iter().collect::<Vec<_>>();
    with_environment_recursive(&overrides, run)
}

fn with_environment_recursive<T>(overrides: &[EnvironmentOverride], run: impl FnOnce() -> T) -> T {
    if overrides.is_empty() {
        return run();
    }
    let override_value = &overrides[0];
    let remaining = &overrides[1..];
    temp_env::with_var(override_value.name, Some(override_value.value), || {
        with_environment_recursive(remaining, run)
    })
}

#[test]
fn service_config_reads_environment() {
    with_environment(
        [
            environment_override("KRABKA_OBSERVABILITY_TARGET", "querier"),
            environment_override("KRABKA_OBSERVABILITY_LISTEN_ADDR", "127.0.0.1:3200"),
            environment_override(
                "KRABKA_OBSERVABILITY_OBJECT_STORE_URL",
                "s3://krabka-observability",
            ),
            environment_override(
                "KRABKA_OBSERVABILITY_WAL_BOOTSTRAP_SERVER",
                "127.0.0.1:9092",
            ),
            environment_override("KRABKA_OBSERVABILITY_WAL_TOPIC", "logs-wal"),
            environment_override("KRABKA_OBSERVABILITY_WAL_GROUP_ID", "logs-querier"),
            environment_override(
                "KRABKA_OBSERVABILITY_DATA_ROOT",
                "/var/lib/krabka-observability",
            ),
            environment_override(
                "KRABKA_OBSERVABILITY_QUERIER_INDEX_SOURCE",
                "tenant-object-store-shards",
            ),
            environment_override("KRABKA_OBSERVABILITY_TENANT", "tenant-a"),
            environment_override("KRABKA_OBSERVABILITY_INDEX_PREFIX", "observability/logs"),
            environment_override("KRABKA_OBSERVABILITY_QUERY_START_NS", "10"),
            environment_override("KRABKA_OBSERVABILITY_QUERY_END_NS", "30"),
            environment_override("KRABKA_OBSERVABILITY_MAX_QUERY_RANGE", "20ns"),
            environment_override("KRABKA_OBSERVABILITY_MAX_QUERY_SERIES", "10"),
            environment_override("KRABKA_OBSERVABILITY_DISCOVER_LOG_LEVELS", "false"),
            environment_override(
                "KRABKA_OBSERVABILITY_LOG_LEVEL_FIELDS",
                "priority,SeverityText",
            ),
            environment_override("KRABKA_OBSERVABILITY_LOG_LEVEL_FROM_JSON_MAX_DEPTH", "-2"),
            environment_override("KRABKA_OBSERVABILITY_MAX_QUERY_READ", "1KiB"),
            environment_override("KRABKA_OBSERVABILITY_MAX_QUERY_STRING_BYTES", "64B"),
            environment_override("KRABKA_OBSERVABILITY_RETENTION_PERIOD", "30d"),
            environment_override("KRABKA_OBSERVABILITY_MAX_INGEST_BODY", "2KiB"),
            environment_override("KRABKA_OBSERVABILITY_WAL_APPEND_TIMEOUT", "250ms"),
            environment_override("KRABKA_OBSERVABILITY_REJECT_OLD_SAMPLES_MAX_AGE", "8d"),
            environment_override("KRABKA_OBSERVABILITY_CREATION_GRACE_PERIOD", "11m"),
            environment_override("KRABKA_OBSERVABILITY_INGEST_QUOTA_BURST_WINDOW", "2s"),
            environment_override("KRABKA_OBSERVABILITY_BROKER_ACCESS_CACHE_TTL", "15s"),
            environment_override("KRABKA_OBSERVABILITY_BROKER_ACCESS_MAX_STALENESS", "2m"),
            environment_override("KRABKA_OBSERVABILITY_BROKER_ACCESS_TENANT_CAPACITY", "500"),
            environment_override("KRABKA_OBSERVABILITY_WAL_CONNECT_STARTUP_DEADLINE", "3m"),
            environment_override("KRABKA_OBSERVABILITY_WAL_CONNECT_ATTEMPT_TIMEOUT", "16s"),
            environment_override("KRABKA_OBSERVABILITY_WAL_CONNECT_INITIAL_BACKOFF", "300ms"),
            environment_override("KRABKA_OBSERVABILITY_WAL_CONNECT_MAX_BACKOFF", "3s"),
            environment_override("KRABKA_OBSERVABILITY_COMPACTOR_WAL_POLL_TIMEOUT", "600ms"),
            environment_override("KRABKA_OBSERVABILITY_COMPACTOR_ACCUMULATION_WINDOW", "3s"),
            environment_override(
                "KRABKA_OBSERVABILITY_COMPACTOR_ACCUMULATION_POLL_TIMEOUT",
                "300ms",
            ),
            environment_override(
                "KRABKA_OBSERVABILITY_COMPACTOR_MAX_RECORDS_PER_BATCH",
                "5000",
            ),
            environment_override("KRABKA_OBSERVABILITY_COMPACTOR_IDLE_INTERVAL", "20ms"),
            environment_override(
                "KRABKA_OBSERVABILITY_COMPACTOR_RETENTION_SWEEP_INTERVAL",
                "5m",
            ),
            environment_override(
                "KRABKA_OBSERVABILITY_COMPACTOR_OBJECT_STORE_INITIAL_BACKOFF",
                "20ms",
            ),
            environment_override(
                "KRABKA_OBSERVABILITY_COMPACTOR_OBJECT_STORE_MAX_BACKOFF",
                "600ms",
            ),
            environment_override(
                "KRABKA_OBSERVABILITY_QUERIER_FRONTIER_REFRESH_INTERVAL",
                "6s",
            ),
            environment_override("KRABKA_OBSERVABILITY_QUERIER_DYNAMIC_INDEX_CACHE_TTL", "7s"),
            environment_override("KRABKA_OBSERVABILITY_QUERIER_SHARD_INDEX_CACHE_TTL", "6m"),
            environment_override("KRABKA_OBSERVABILITY_QUERIER_SHARD_FETCH_CONCURRENCY", "33"),
            environment_override(
                "KRABKA_OBSERVABILITY_QUERIER_COLD_BLOCK_FETCH_CONCURRENCY",
                "9",
            ),
            environment_override("KRABKA_OBSERVABILITY_QUERIER_HOT_TAIL_BUCKET_WIDTH", "2m"),
            environment_override("KRABKA_OBSERVABILITY_QUERIER_HOT_TAIL_INTERVAL", "60ms"),
            environment_override(
                "KRABKA_OBSERVABILITY_QUERIER_DEPENDENCY_RECONNECT_INTERVAL",
                "600ms",
            ),
        ],
        || {
            let config =
                ServiceConfig::try_parse_from(["krabka-observability"]).expect("parse environment");

            assert_environment_config(&config);
        },
    );
}

fn assert_environment_config(config: &ServiceConfig) {
    assert!(
        config
            == &ServiceConfig {
                wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                wal_topic: "logs-wal".to_string(),
                wal_group_id: "logs-querier".to_string(),
                discover_log_levels: false,
                log_level_fields: vec!["priority".into(), "SeverityText".into()],
                log_level_from_json_max_depth: -2,
                enable_multi_variant_queries: false,
                shard_aggregations: Vec::new(),
                max_count_min_sketch_heap_size: 10_000,
                retention_period: Some(days(30)),
                logs_limits_overrides_config: None,
                max_ingest_body: Some(kibibytes(2)),
                wal_append_timeout: Some(millis(250)),
                reject_old_samples_max_age: days(8),
                creation_grace_period: minutes(11),
                ingest_quota_burst_window: secs(2),
                broker_access_cache_ttl: secs(15),
                broker_access_max_staleness: minutes(2),
                broker_access_tenant_capacity: NonZeroUsize::new(500).unwrap(),
                wal_connect_startup_deadline: minutes(3),
                wal_connect_attempt_timeout: secs(16),
                wal_connect_initial_backoff: millis(300),
                wal_connect_max_backoff: secs(3),
                compactor_wal_poll_timeout: millis(600),
                compactor_accumulation_window: secs(3),
                compactor_accumulation_poll_timeout: millis(300),
                compactor_max_records_per_batch: NonZeroUsize::new(5000).unwrap(),
                compactor_idle_interval: millis(20),
                compactor_retention_sweep_interval: minutes(5),
                all_drain_stage_timeout: secs(30),
                compactor_object_store_initial_backoff: millis(20),
                compactor_object_store_max_backoff: millis(600),
                querier_frontier_refresh_interval: secs(6),
                querier_dynamic_index_cache_ttl: secs(7),
                querier_shard_index_cache_ttl: minutes(6),
                querier_shard_fetch_concurrency: NonZeroUsize::new(33).unwrap(),
                querier_cold_block_fetch_concurrency: NonZeroUsize::new(9).unwrap(),
                querier_query_frontend_split_interval: hours(1),
                querier_query_frontend_max_parallelism: NonZeroUsize::new(14).unwrap(),
                querier_query_frontend_max_retries: 5,
                querier_query_frontend_cache_ttl: days(7),
                querier_query_frontend_max_cache_freshness: minutes(10),
                querier_query_frontend_target_bytes_per_shard: mebibytes(600),
                querier_hot_tail_bucket_width: minutes(2),
                querier_hot_tail_interval: millis(60),
                querier_dependency_reconnect_interval: millis(600),
                server_security: ServerSecurityArgs::default(),
                audit: AuditArgs::default(),
                wal_client_security: WalClientSecurityArgs::default(),
                ..scoped_querier_config()
            }
    );
}

use std::num::NonZeroUsize;

use clap::Parser;
use krabka_observability::{QuerierIndexSource, Role, ServiceConfig};
use krabka_units::{bytes, days, kibibytes, millis, minutes, nanos, secs};

#[test]
fn service_config_reads_environment() {
    temp_env::with_vars(
        [
            ("KRABKA_OBSERVABILITY_TARGET", Some("querier")),
            ("KRABKA_OBSERVABILITY_LISTEN_ADDR", Some("127.0.0.1:3200")),
            (
                "KRABKA_OBSERVABILITY_OBJECT_STORE_URL",
                Some("s3://krabka-observability"),
            ),
            (
                "KRABKA_OBSERVABILITY_WAL_BOOTSTRAP_SERVER",
                Some("127.0.0.1:9092"),
            ),
            ("KRABKA_OBSERVABILITY_WAL_TOPIC", Some("logs-wal")),
            ("KRABKA_OBSERVABILITY_WAL_GROUP_ID", Some("logs-querier")),
            (
                "KRABKA_OBSERVABILITY_DATA_ROOT",
                Some("/var/lib/krabka-observability"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_INDEX_SOURCE",
                Some("tenant-object-store-shards"),
            ),
            ("KRABKA_OBSERVABILITY_TENANT", Some("tenant-a")),
            (
                "KRABKA_OBSERVABILITY_INDEX_PREFIX",
                Some("observability/logs"),
            ),
            ("KRABKA_OBSERVABILITY_QUERY_START_NS", Some("10")),
            ("KRABKA_OBSERVABILITY_QUERY_END_NS", Some("30")),
            ("KRABKA_OBSERVABILITY_MAX_QUERY_RANGE", Some("20ns")),
            ("KRABKA_OBSERVABILITY_MAX_QUERY_SERIES", Some("10")),
            ("KRABKA_OBSERVABILITY_MAX_QUERY_READ", Some("1KiB")),
            ("KRABKA_OBSERVABILITY_MAX_QUERY_LENGTH", Some("64B")),
            ("KRABKA_OBSERVABILITY_MAX_INGEST_BODY", Some("2KiB")),
            ("KRABKA_OBSERVABILITY_WAL_APPEND_TIMEOUT", Some("250ms")),
            (
                "KRABKA_OBSERVABILITY_REJECT_OLD_SAMPLES_MAX_AGE",
                Some("8d"),
            ),
            ("KRABKA_OBSERVABILITY_CREATION_GRACE_PERIOD", Some("11m")),
            ("KRABKA_OBSERVABILITY_INGEST_QUOTA_BURST_WINDOW", Some("2s")),
            (
                "KRABKA_OBSERVABILITY_WAL_CONNECT_STARTUP_DEADLINE",
                Some("3m"),
            ),
            (
                "KRABKA_OBSERVABILITY_WAL_CONNECT_ATTEMPT_TIMEOUT",
                Some("16s"),
            ),
            (
                "KRABKA_OBSERVABILITY_WAL_CONNECT_INITIAL_BACKOFF",
                Some("300ms"),
            ),
            ("KRABKA_OBSERVABILITY_WAL_CONNECT_MAX_BACKOFF", Some("3s")),
            (
                "KRABKA_OBSERVABILITY_COMPACTOR_WAL_POLL_TIMEOUT",
                Some("600ms"),
            ),
            (
                "KRABKA_OBSERVABILITY_COMPACTOR_ACCUMULATION_WINDOW",
                Some("3s"),
            ),
            (
                "KRABKA_OBSERVABILITY_COMPACTOR_ACCUMULATION_POLL_TIMEOUT",
                Some("300ms"),
            ),
            (
                "KRABKA_OBSERVABILITY_COMPACTOR_MAX_RECORDS_PER_BATCH",
                Some("5000"),
            ),
            ("KRABKA_OBSERVABILITY_COMPACTOR_IDLE_INTERVAL", Some("20ms")),
            (
                "KRABKA_OBSERVABILITY_COMPACTOR_OBJECT_STORE_INITIAL_BACKOFF",
                Some("20ms"),
            ),
            (
                "KRABKA_OBSERVABILITY_COMPACTOR_OBJECT_STORE_MAX_BACKOFF",
                Some("600ms"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_FRONTIER_REFRESH_INTERVAL",
                Some("6s"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_DYNAMIC_INDEX_CACHE_TTL",
                Some("7s"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_SHARD_INDEX_CACHE_TTL",
                Some("6m"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_SHARD_FETCH_CONCURRENCY",
                Some("33"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_COLD_BLOCK_FETCH_CONCURRENCY",
                Some("9"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_HOT_TAIL_BUCKET_WIDTH",
                Some("2m"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_HOT_TAIL_INTERVAL",
                Some("60ms"),
            ),
            (
                "KRABKA_OBSERVABILITY_QUERIER_DEPENDENCY_RECONNECT_INTERVAL",
                Some("600ms"),
            ),
        ],
        || {
            let config =
                ServiceConfig::try_parse_from(["krabka-observability"]).expect("parse environment");

            assert_eq!(
                config,
                ServiceConfig {
                    target: Role::Querier,
                    listen_addr: "127.0.0.1:3200".parse().unwrap(),
                    object_store_url: Some("s3://krabka-observability".to_string()),
                    wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                    wal_topic: "logs-wal".to_string(),
                    wal_group_id: "logs-querier".to_string(),
                    data_root: "/var/lib/krabka-observability".into(),
                    querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
                    tenant: Some("tenant-a".to_string()),
                    index_prefix: Some("observability/logs".to_string()),
                    query_start_ns: Some(10),
                    query_end_ns: Some(30),
                    max_query_range: Some(nanos(20)),
                    max_query_series: Some(10),
                    max_query_read: Some(kibibytes(1)),
                    max_query_length: Some(bytes(64)),
                    max_ingest_body: Some(kibibytes(2)),
                    wal_append_timeout: Some(millis(250)),
                    reject_old_samples_max_age: days(8),
                    creation_grace_period: minutes(11),
                    ingest_quota_burst_window: secs(2),
                    wal_connect_startup_deadline: minutes(3),
                    wal_connect_attempt_timeout: secs(16),
                    wal_connect_initial_backoff: millis(300),
                    wal_connect_max_backoff: secs(3),
                    compactor_wal_poll_timeout: millis(600),
                    compactor_accumulation_window: secs(3),
                    compactor_accumulation_poll_timeout: millis(300),
                    compactor_max_records_per_batch: NonZeroUsize::new(5000).unwrap(),
                    compactor_idle_interval: millis(20),
                    compactor_object_store_initial_backoff: millis(20),
                    compactor_object_store_max_backoff: millis(600),
                    querier_frontier_refresh_interval: secs(6),
                    querier_dynamic_index_cache_ttl: secs(7),
                    querier_shard_index_cache_ttl: minutes(6),
                    querier_shard_fetch_concurrency: NonZeroUsize::new(33).unwrap(),
                    querier_cold_block_fetch_concurrency: NonZeroUsize::new(9).unwrap(),
                    querier_hot_tail_bucket_width: minutes(2),
                    querier_hot_tail_interval: millis(60),
                    querier_dependency_reconnect_interval: millis(600),
                }
            );
        },
    );
}

use std::{net::SocketAddr, process::ExitCode, sync::Arc};

use arc_swap::ArcSwap;
use clap::{ArgAction, Args, Parser, ValueEnum};
use krabka_blockstore::{
    BlockStore, BlockWriter, IndexSnapshotRetain, PromotedSpanAttr, TraceIndex,
};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerFetchMaxBytes};
use krabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_client_producer::Producer;
use krabka_telemetry::OtlpConfig;
use krabka_traceql::{EngineOpts, TraceqlEngine};
use krabka_traces::{
    LiveStore, TRACES_WAL_TOPIC, blockbuilder,
    compactor::compact_index_window_with_max_bytes,
    distributor::{self, DistributorState, KafkaSink},
    frontend::{self, FrontendConfig, TraceIndexCatalog},
    ids::UnixNano,
    livestore,
    metrics::ServiceMetrics,
    metricsgen::{
        KafkaSpanSource, MetricsGenConfig, MetricsGenService, PrometheusRemoteWriteSink,
        SystemClock,
    },
    querier::{
        self as trace_querier,
        http::HttpConfig,
        live::{LiveSource, LiveTier, RemoteLiveSource},
        store::{DEFAULT_SCAN_CONCAT_MAX, KrabkaSpanStore, SharedTraceIndex},
    },
    span::batch::RESOURCE_ATTR_PREFIX,
};
use krabka_units::{
    ByteSize, Frequency, Time,
    convert::{ByteSizeExt as _, FrequencyExt, TimeExt as _},
    kibibytes, parse,
};
use num_traits::ToPrimitive as _;
use object_store::{ObjectStore, path::Path};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use url::Url;

#[cfg(test)]
mod tests {
    use assert2::check;
    use axum::{
        body::Body,
        http::{Request, StatusCode as HttpStatusCode},
    };
    use clap::{CommandFactory as _, Parser};
    use http_body_util::BodyExt;
    use krabka_units::{minutes, secs};
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn non_dimensioned_cli_arguments_have_environment_backing() {
        let command = Cli::command();
        for (id, env) in [
            ("target", "KRABKA_TRACES_TARGET"),
            ("listen", "KRABKA_TRACES_LISTEN"),
            ("grpc_listen", "KRABKA_TRACES_GRPC_LISTEN"),
            ("otlp_http_listen", "KRABKA_TRACES_OTLP_HTTP_LISTEN"),
            ("jaeger_grpc_listen", "KRABKA_TRACES_JAEGER_GRPC_LISTEN"),
            (
                "jaeger_compact_listen",
                "KRABKA_TRACES_JAEGER_COMPACT_LISTEN",
            ),
            ("jaeger_http_listen", "KRABKA_TRACES_JAEGER_HTTP_LISTEN"),
            ("zipkin_listen", "KRABKA_TRACES_ZIPKIN_LISTEN"),
            ("bootstrap", "KRABKA_TRACES_BOOTSTRAP"),
            ("querier_live_store", "KRABKA_TRACES_QUERIER_LIVE_STORE"),
            (
                "querier_live_store_url",
                "KRABKA_TRACES_QUERIER_LIVE_STORE_URL",
            ),
            ("trace_index_key", "KRABKA_TRACES_TRACE_INDEX_KEY"),
            ("object_store_url", "KRABKA_TRACES_OBJECT_STORE_URL"),
            ("remote_write_url", "KRABKA_TRACES_REMOTE_WRITE_URL"),
            (
                "max_exemplars_per_series",
                "KRABKA_TRACES_MAX_EXEMPLARS_PER_SERIES",
            ),
            ("edge_store_max_items", "KRABKA_TRACES_EDGE_STORE_MAX_ITEMS"),
            ("querier_url", "KRABKA_TRACES_QUERIER_URL"),
            ("query_queue_depth", "KRABKA_TRACES_QUERY_QUEUE_DEPTH"),
            ("max_trace_spans", "KRABKA_TRACES_MAX_TRACE_SPANS"),
            (
                "max_spans_per_request",
                "KRABKA_TRACES_MAX_SPANS_PER_REQUEST",
            ),
            ("max_spans_per_trace", "KRABKA_TRACES_MAX_SPANS_PER_TRACE"),
            (
                "max_ingest_spans_per_second",
                "KRABKA_TRACES_MAX_INGEST_SPANS_PER_SECOND",
            ),
            ("ingest_rate_burst", "KRABKA_TRACES_INGEST_RATE_BURST"),
            ("promote_span_attrs", "KRABKA_TRACES_PROMOTE_SPAN_ATTR"),
            (
                "promote_resource_attrs",
                "KRABKA_TRACES_PROMOTE_RESOURCE_ATTR",
            ),
            ("config", "KRABKA_TRACES_CONFIG"),
            ("enable_target_info", "KRABKA_TRACES_ENABLE_TARGET_INFO"),
            (
                "enable_status_message",
                "KRABKA_TRACES_ENABLE_STATUS_MESSAGE",
            ),
            (
                "enable_messaging_system_latency",
                "KRABKA_TRACES_ENABLE_MESSAGING_SYSTEM_LATENCY",
            ),
        ] {
            let configured = command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_env())
                .and_then(|value| value.to_str());
            check!(configured == Some(env), "missing {env} on {id}");
        }
    }

    #[test]
    fn every_process_argument_has_environment_backing() {
        let command = Cli::command();
        let missing = command
            .get_arguments()
            .filter(|arg| arg.get_env().is_none())
            .map(|arg| arg.get_id().to_string())
            .collect::<Vec<_>>();
        check!(
            missing.is_empty(),
            "arguments without environment backing: {missing:?}"
        );
    }

    #[test]
    fn process_environment_supplies_cli_and_explicit_flags_win() {
        const CHILD: &str = "KRABKA_TRACES_PROCESS_ENVIRONMENT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::process_environment_supplies_cli_and_explicit_flags_win",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_TARGET", "querier")
                    .env("KRABKA_TRACES_LISTEN", "127.0.0.1:3210")
                    .env("KRABKA_TRACES_ENABLE_TARGET_INFO", "true")
                    .env(
                        "KRABKA_TRACES_PROMOTE_SPAN_ATTR",
                        "http.method:string,http.status:int",
                    )
                    .env("KRABKA_TRACES_QUERY_QUEUE_DEPTH", "7")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces"]).unwrap();
        check!(
            (
                from_env.target,
                from_env.listen.as_str(),
                from_env.metrics.enable_target_info,
                from_env.promote_span_attrs.as_slice(),
                from_env.query_queue_depth,
            ) == (
                Target::Querier,
                "127.0.0.1:3210",
                true,
                &[
                    "http.method:string".to_string(),
                    "http.status:int".to_string()
                ][..],
                7,
            )
        );

        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=query-frontend",
            "--listen=127.0.0.1:3220",
            "--query-queue-depth=11",
        ])
        .unwrap();
        check!(
            (
                from_cli.target,
                from_cli.listen.as_str(),
                from_cli.query_queue_depth
            ) == (Target::QueryFrontend, "127.0.0.1:3220", 11)
        );
    }

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults = Cli::try_parse_from(["krabka-traces", "--target", "querier"]).unwrap();
        assert2::assert!(defaults.client_dispatch_queue_capacity == 64);
        assert2::assert!(defaults.client_frame_max == krabka_units::mebibytes(100));

        let custom = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
        ])
        .unwrap();
        assert2::assert!(custom.client_dispatch_queue_capacity == 7);
        assert2::assert!(custom.client_frame_max == kibibytes(32));

        for args in [
            vec![
                "krabka-traces",
                "--target",
                "querier",
                "--client-dispatch-queue-capacity",
                "0",
            ],
            vec![
                "krabka-traces",
                "--target",
                "querier",
                "--client-frame-max",
                "101MiB",
            ],
        ] {
            assert2::assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("KRABKA_TRACES_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target", "querier"]).unwrap();
        assert2::assert!(from_env.client_dispatch_queue_capacity == 7);
        assert2::assert!(from_env.client_frame_max == kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ])
        .unwrap();
        assert2::assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert2::assert!(from_cli.client_frame_max == kibibytes(64));
    }

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "distributor"]).unwrap();
        assert2::assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn parses_distributor_grpc_listener() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--grpc-listen",
            "127.0.0.1:4317",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.grpc_listen.as_str() == "127.0.0.1:4317");
    }

    #[test]
    fn parses_distributor_jaeger_compact_listener() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--jaeger-compact-listen",
            "127.0.0.1:6831",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.jaeger_compact_listen.as_str() == "127.0.0.1:6831");
    }

    #[test]
    fn parses_distributor_jaeger_grpc_listener() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--jaeger-grpc-listen",
            "127.0.0.1:14250",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.jaeger_grpc_listen.as_str() == "127.0.0.1:14250");
    }

    #[test]
    fn distributor_defaults_include_tempo_push_ports() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "distributor"]).unwrap();

        assert2::assert!(cli.otlp_http_listen.as_str() == "127.0.0.1:4318");
        assert2::assert!(cli.jaeger_grpc_listen.as_str() == "127.0.0.1:14250");
        assert2::assert!(cli.jaeger_http_listen.as_str() == "127.0.0.1:14268");
        assert2::assert!(cli.zipkin_listen.as_str() == "127.0.0.1:9411");
    }

    #[test]
    fn parses_distributor_ingest_limits() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--max-spans-per-request",
            "123",
            "--max-attr-value-len",
            "456",
            "--max-decompressed-bytes",
            "789",
        ])
        .unwrap();

        assert2::assert!(cli.max_spans_per_request == 123);
        assert2::assert!(cli.max_attr_value_len == ByteSize::from_bytes(456));
        assert2::assert!(cli.max_decompressed_bytes == ByteSize::from_bytes(789));
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "block-builder"]).unwrap();
        assert2::assert!(matches!(cli.target, Target::BlockBuilder));
    }

    #[test]
    fn parses_block_builder_flush_window() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "block-builder",
            "--block-builder-window-secs",
            "30",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::BlockBuilder));
        assert2::assert!(cli.block_builder_window == secs(30));
    }

    #[test]
    fn block_builder_flush_knobs_default() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "block-builder"]).unwrap();

        check!(cli.block_builder_empty_poll_backoff == krabka_units::millis(100));
        assert2::assert!(
            cli.block_builder_flush_max_records
                == krabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_RECORDS
        );
        assert2::assert!(
            cli.block_builder_flush_max_age == krabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_AGE
        );
    }

    #[test]
    fn block_builder_empty_poll_backoff_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_BLOCK_BUILDER_EMPTY_POLL_BACKOFF_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::block_builder_empty_poll_backoff_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_BLOCK_BUILDER_EMPTY_POLL_BACKOFF", "7ms")
                    .env("KRABKA_TRACES_BLOCK_BUILDER_FLUSH_MAX_RECORDS", "17")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target=block-builder"]).unwrap();
        check!(
            (
                from_env.block_builder_empty_poll_backoff,
                from_env.block_builder_flush_max_records,
            ) == (krabka_units::millis(7), 17)
        );
        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=block-builder",
            "--block-builder-empty-poll-backoff=11ms",
            "--block-builder-flush-max-records=19",
        ])
        .unwrap();
        check!(
            (
                from_cli.block_builder_empty_poll_backoff,
                from_cli.block_builder_flush_max_records,
            ) == (krabka_units::millis(11), 19)
        );
        check!(
            Cli::try_parse_from([
                "krabka-traces",
                "--target=block-builder",
                "--block-builder-empty-poll-backoff=0ms",
            ])
            .is_err()
        );
        check!(
            Cli::try_parse_from([
                "krabka-traces",
                "--target=block-builder",
                "--block-builder-flush-max-records=0",
            ])
            .is_err()
        );
    }

    #[test]
    fn index_snapshot_policy_defaults_and_rejects_invalid_values() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "block-builder"]).unwrap();
        assert_eq!(
            cli.index_snapshot_max,
            krabka_blockstore::DEFAULT_INDEX_SNAPSHOT_MAX
        );
        assert_eq!(
            cli.index_snapshot_retain.into_value(),
            krabka_blockstore::DEFAULT_INDEX_SNAPSHOT_RETAIN
        );

        for flag in ["--index-snapshot-max", "--index-snapshot-retain"] {
            for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
                assert!(
                    Cli::try_parse_from([
                        "krabka-traces",
                        "--target",
                        "block-builder",
                        flag,
                        invalid,
                    ])
                    .is_err(),
                    "{flag} should reject {invalid:?}"
                );
            }
        }
        for invalid in ["1.5B", "18446744073709551616B"] {
            assert!(
                Cli::try_parse_from([
                    "krabka-traces",
                    "--target",
                    "block-builder",
                    "--index-snapshot-max",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn index_snapshot_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_INDEX_SNAPSHOT_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::index_snapshot_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_INDEX_SNAPSHOT_MAX", "1KiB")
                    .env("KRABKA_TRACES_INDEX_SNAPSHOT_RETAIN", "3")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.index_snapshot_max.bytes_u64(), 1024);
        assert_eq!(from_env.index_snapshot_retain.into_value(), 3);

        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "block-builder",
            "--index-snapshot-max",
            "2KiB",
            "--index-snapshot-retain",
            "4",
        ])
        .unwrap();
        assert_eq!(from_cli.index_snapshot_max.bytes_u64(), 2048);
        assert_eq!(from_cli.index_snapshot_retain.into_value(), 4);
    }

    #[test]
    fn block_read_max_defaults_and_rejects_invalid_values() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "querier"]).unwrap();
        assert_eq!(
            cli.block_read_max,
            krabka_blockstore::DEFAULT_BLOCK_READ_MAX
        );

        for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
            assert!(
                Cli::try_parse_from([
                    "krabka-traces",
                    "--target",
                    "querier",
                    "--block-read-max",
                    invalid,
                ])
                .is_err(),
                "--block-read-max should reject {invalid:?}"
            );
        }
        for invalid in ["1.5B", "18446744073709551616B"] {
            assert!(
                Cli::try_parse_from([
                    "krabka-traces",
                    "--target",
                    "querier",
                    "--block-read-max",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn block_read_max_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_BLOCK_READ_MAX_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::block_read_max_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_BLOCK_READ_MAX", "1KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target", "querier"]).unwrap();
        assert_eq!(from_env.block_read_max.bytes_u64(), 1024);

        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--block-read-max",
            "2KiB",
        ])
        .unwrap();
        assert_eq!(from_cli.block_read_max.bytes_u64(), 2048);
    }

    #[test]
    fn scan_concat_max_preserves_default_and_rejects_invalid_values() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "querier"]).unwrap();
        assert_eq!(cli.scan_concat_max.bytes_u64(), 1_500_000_000);

        for invalid in [
            "0",
            "not-a-number",
            "-1B",
            "1500000001B",
            "18446744073709551616B",
        ] {
            assert!(
                Cli::try_parse_from([
                    "krabka-traces",
                    "--target",
                    "querier",
                    "--scan-concat-max",
                    invalid,
                ])
                .is_err(),
                "--scan-concat-max should reject {invalid:?}"
            );
        }
    }

    #[test]
    fn scan_concat_max_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_SCAN_CONCAT_MAX_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::scan_concat_max_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_SCAN_CONCAT_MAX", "1KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target", "querier"]).unwrap();
        assert_eq!(from_env.scan_concat_max.bytes_u64(), 1024);

        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--scan-concat-max",
            "2KiB",
        ])
        .unwrap();
        assert_eq!(from_cli.scan_concat_max.bytes_u64(), 2048);
    }

    #[test]
    fn wal_fetch_limits_preserve_defaults_and_reject_invalid_values() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "block-builder"]).unwrap();
        assert_eq!(cli.wal_fetch_max.bytes_i32(), 2_097_152);
        assert_eq!(cli.wal_fetch_partition_max.bytes_i32(), 262_144);

        for (flag, invalid) in [
            ("--wal-fetch-max", "0"),
            ("--wal-fetch-max", "not-a-number"),
            ("--wal-fetch-max", "-1B"),
            ("--wal-fetch-max", "1.5B"),
            ("--wal-fetch-max", "2147483648B"),
            ("--wal-fetch-partition-max", "0"),
            ("--wal-fetch-partition-max", "not-a-number"),
            ("--wal-fetch-partition-max", "-1B"),
            ("--wal-fetch-partition-max", "1.5B"),
            ("--wal-fetch-partition-max", "2147483648B"),
        ] {
            assert!(
                Cli::try_parse_from(["krabka-traces", "--target", "block-builder", flag, invalid,])
                    .is_err(),
                "{flag} should reject {invalid:?}"
            );
        }
    }

    #[test]
    fn wal_fetch_limits_read_environment_and_prefer_cli() {
        const CHILD: &str = "KRABKA_TRACES_WAL_FETCH_LIMITS_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::wal_fetch_limits_read_environment_and_prefer_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_WAL_FETCH_MAX", "1KiB")
                    .env("KRABKA_TRACES_WAL_FETCH_PARTITION_MAX", "256B")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.wal_fetch_max.bytes_i32(), 1024);
        assert_eq!(from_env.wal_fetch_partition_max.bytes_i32(), 256);

        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "block-builder",
            "--wal-fetch-max",
            "2KiB",
            "--wal-fetch-partition-max",
            "512B",
        ])
        .unwrap();
        assert_eq!(from_cli.wal_fetch_max.bytes_i32(), 2048);
        assert_eq!(from_cli.wal_fetch_partition_max.bytes_i32(), 512);
    }

    #[test]
    fn parses_block_builder_flush_knobs() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "block-builder",
            "--block-builder-flush-max-records",
            "1000",
            "--block-builder-flush-max-age-ms",
            "30000",
        ])
        .unwrap();

        check!(
            (
                cli.target,
                cli.block_builder_flush_max_records,
                cli.block_builder_flush_max_age,
            ) == (Target::BlockBuilder, 1000, secs(30))
        );
    }

    #[test]
    fn parses_block_builder_promoted_attrs() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "block-builder",
            "--promote-resource-attr",
            "service.name:string",
            "--promote-span-attr",
            "http.status_code:int",
            "--promote-span-attr",
            "http.method",
        ])
        .unwrap();

        let promoted = promoted_attrs_from_cli(&cli).unwrap();
        check!(
            promoted
                == vec![
                    krabka_blockstore::PromotedSpanAttr::string("__resource.service.name"),
                    krabka_blockstore::PromotedSpanAttr::int("http.status_code"),
                    krabka_blockstore::PromotedSpanAttr::string("http.method"),
                ]
        );
    }

    #[test]
    fn rejects_unknown_promoted_attr_type() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "block-builder",
            "--promote-span-attr",
            "http.method:bytes",
        ])
        .unwrap();

        assert2::assert!(promoted_attrs_from_cli(&cli).is_err());
    }

    #[test]
    fn rejects_unknown_target() {
        assert2::assert!(Cli::try_parse_from(["krabka-traces", "--target", "bogus"]).is_err());
    }

    #[test]
    fn parses_live_store_retention() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "live-store",
            "--retention-ns",
            "42",
        ])
        .unwrap();
        assert2::assert!(matches!(cli.target, Target::LiveStore));
        assert2::assert!(cli.retention == Time::from_nanos(42));
    }

    #[test]
    fn parses_querier_live_store_option() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--querier-live-store",
            "--retention-ns",
            "42",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.querier_live_store);
        assert2::assert!(cli.retention == Time::from_nanos(42));
    }

    #[test]
    fn parses_querier_remote_live_store_url() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--querier-live-store-url",
            "http://127.0.0.1:3201",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.querier_live_store_url.as_deref() == Some("http://127.0.0.1:3201"));
    }

    #[tokio::test]
    async fn live_store_router_serves_recent_trace_by_id() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(krabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([7; 16], [3; 8]),
        });
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "live-store"]).unwrap();
        let router = build_live_store_router(&cli, store).unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/07070707070707070707070707070707")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(response.status() == HttpStatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        check!(json["status"] == "COMPLETE");
        check!(
            json["trace"]["resourceSpans"][0]["resource"]["attributes"][0]["key"] == "service.name"
        );
        check!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"] == "GET /live"
        );
    }

    #[tokio::test]
    async fn remote_live_source_reads_batches_from_live_store_router() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(krabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([8; 16], [4; 8]),
        });
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "live-store"]).unwrap();
        let router = build_live_store_router(&cli, store).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant-a",
            krabka_blockstore::TraceBlockStats {
                object_key: "blocks/cold.parquet".into(),
                min_ts: 0,
                max_ts: 999,
                bloom: krabka_blockstore::ShardedTraceBloom::new(1, 1, 0.01),
                tag_names: std::collections::BTreeSet::default(),
                tag_values: std::collections::BTreeMap::default(),
            },
        );
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(index)),
        );

        let batches = source.span_batches("tenant-a", 1_000, 2_000).await.unwrap();

        assert2::assert!(source.block_builder_frontier_ns("tenant-a") == 1_000);
        assert2::assert!(
            batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>()
                == 1
        );
        server.abort();
    }

    #[tokio::test]
    async fn remote_live_source_reads_trace_by_id_from_live_store_router() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(krabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([9; 16], [5; 8]),
        });
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "live-store"]).unwrap();
        let router = build_live_store_router(&cli, store).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(TraceIndex::new())),
        );

        let trace = source
            .trace_spans("tenant-a", &[9; 16])
            .await
            .unwrap()
            .unwrap();

        check!(trace.trace_id == [9; 16]);
        check!(trace.root_service_name == "live-api");
        check!(trace.spans[0].span_id == [5; 8]);
        check!(trace.spans[0].name == "GET /live");
        server.abort();
    }

    #[tokio::test]
    async fn remote_live_source_reads_tags_and_values_from_live_store_router() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(krabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([11; 16], [7; 8]),
        });
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "live-store"]).unwrap();
        let router = build_live_store_router(&cli, store).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(TraceIndex::new())),
        );

        let tags = source
            .tag_names(
                "tenant-a",
                Some(krabka_traceql::TagScope::Resource),
                0,
                2_000,
            )
            .await
            .unwrap();
        let values = source
            .tag_values("tenant-a", "resource.service.name", 0, 2_000)
            .await
            .unwrap();

        assert2::assert!(
            tags.iter()
                .any(|scope| scope.tags.iter().any(|tag| tag == "service.name"))
        );
        assert2::assert!(values.iter().any(|value| value.value == "live-api"));
        server.abort();
    }

    #[tokio::test]
    async fn querier_router_federates_remote_live_store_by_id() {
        let store = Arc::new(RwLock::new(LiveStore::new(i64::MAX)));
        store.write().await.ingest(krabka_traces::SpanRecord {
            tenant: "tenant-a".into(),
            span: test_span([10; 16], [6; 8]),
        });
        let live_cli = Cli::try_parse_from(["krabka-traces", "--target", "live-store"]).unwrap();
        let live_router = build_live_store_router(&live_cli, store).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, live_router).await.unwrap();
        });
        let querier_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--querier-live-store-url",
            &format!("http://{addr}"),
        ])
        .unwrap();
        let router = build_querier_router(&querier_cli).await.unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(response.status() == HttpStatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(
            json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["spanId"]
                == "BgYGBgYGBgY="
        );
        server.abort();
    }

    #[tokio::test]
    async fn indexed_live_source_uses_trace_index_max_timestamp_as_frontier() {
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant-a",
            krabka_blockstore::TraceBlockStats {
                object_key: "blocks/a.parquet".into(),
                min_ts: 100,
                max_ts: 499,
                bloom: krabka_blockstore::ShardedTraceBloom::new(1, 1, 0.01),
                tag_names: std::collections::BTreeSet::default(),
                tag_values: std::collections::BTreeMap::default(),
            },
        );
        index.add_trace_block(
            "tenant-a",
            krabka_blockstore::TraceBlockStats {
                object_key: "blocks/b.parquet".into(),
                min_ts: 500,
                max_ts: 750,
                bloom: krabka_blockstore::ShardedTraceBloom::new(1, 1, 0.01),
                tag_names: std::collections::BTreeSet::default(),
                tag_values: std::collections::BTreeMap::default(),
            },
        );
        let source = IndexedLiveSource::new(
            Arc::new(RwLock::new(LiveStore::new(i64::MAX))),
            Arc::new(ArcSwap::from_pointee(index)),
        );

        assert2::assert!(source.block_builder_frontier_ns("tenant-a") == 751);
        assert2::assert!(source.block_builder_frontier_ns("tenant-b") == 0);
    }

    fn test_span(trace_id: [u8; 16], span_id: [u8; 8]) -> krabka_traces::Span {
        krabka_traces::Span {
            trace_id,
            span_id,
            parent_span_id: None,
            name: "GET /live".into(),
            kind: krabka_traces::SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: krabka_traces::StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![krabka_traces::KeyValue {
                key: "service.name".into(),
                value: krabka_traces::AttrValue::Str("live-api".into()),
            }],
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[test]
    fn parses_metrics_generator_options() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "metrics-generator",
            "--remote-write-url",
            "http://mimir.example/api/v1/push",
            "--collection-interval-secs",
            "30",
            "--max-exemplars-per-series",
            "3",
            "--edge-ttl-secs",
            "20",
            "--edge-store-max-items",
            "1234",
            "--histogram-buckets-ns",
            "1000,2000,5000",
            "--config",
            "metricsgen.yaml",
        ])
        .unwrap();

        check!(
            (
                cli.target,
                cli.remote_write_url.as_deref(),
                cli.collection_interval,
                cli.max_exemplars_per_series,
                cli.edge_ttl,
                cli.edge_store_max_items,
                cli.histogram_buckets,
                cli.config.as_deref(),
            ) == (
                Target::MetricsGenerator,
                Some("http://mimir.example/api/v1/push"),
                Some(secs(30)),
                Some(3),
                Some(secs(20)),
                Some(1234),
                Some(vec![
                    Time::from_nanos(1000),
                    Time::from_nanos(2000),
                    Time::from_nanos(5000)
                ]),
                Some("metricsgen.yaml"),
            )
        );
    }

    #[test]
    fn duration_policy_reads_uom_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_DURATION_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::duration_policy_reads_uom_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_RETENTION", "42s")
                    .env("KRABKA_TRACES_BLOCK_BUILDER_WINDOW", "7s")
                    .env("KRABKA_TRACES_BLOCK_BUILDER_FLUSH_MAX_AGE", "8s")
                    .env("KRABKA_TRACES_COLLECTION_INTERVAL", "9s")
                    .env("KRABKA_TRACES_EDGE_TTL", "10s")
                    .env("KRABKA_TRACES_HISTOGRAM_BUCKETS", "1ms,2ms")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["krabka-traces", "--target=metrics-generator"]).unwrap();
        check!(
            (
                from_env.retention,
                from_env.block_builder_window,
                from_env.block_builder_flush_max_age,
                from_env.collection_interval,
                from_env.edge_ttl,
                from_env.histogram_buckets,
            ) == (
                secs(42),
                secs(7),
                secs(8),
                Some(secs(9)),
                Some(secs(10)),
                Some(vec![krabka_units::millis(1), krabka_units::millis(2)]),
            )
        );

        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=metrics-generator",
            "--retention=11s",
            "--block-builder-window=12s",
            "--block-builder-flush-max-age=13s",
            "--collection-interval=14s",
            "--edge-ttl=15s",
            "--histogram-buckets=3ms,4ms",
        ])
        .unwrap();
        check!(
            (
                from_cli.retention,
                from_cli.block_builder_window,
                from_cli.block_builder_flush_max_age,
                from_cli.collection_interval,
                from_cli.edge_ttl,
                from_cli.histogram_buckets,
            ) == (
                secs(11),
                secs(12),
                secs(13),
                Some(secs(14)),
                Some(secs(15)),
                Some(vec![krabka_units::millis(3), krabka_units::millis(4)]),
            )
        );
        check!(
            Cli::try_parse_from([
                "krabka-traces",
                "--target=metrics-generator",
                "--collection-interval=0s",
            ])
            .is_err()
        );
    }

    #[test]
    fn byte_policy_reads_uom_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_BYTE_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::byte_policy_reads_uom_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_TARGET_BYTES_PER_JOB", "1KiB")
                    .env("KRABKA_TRACES_MAX_ATTR_VALUE_LEN", "2KiB")
                    .env("KRABKA_TRACES_MAX_DECOMPRESSED_BYTES", "3KiB")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target=query-frontend"]).unwrap();
        check!(
            (
                from_env.target_bytes_per_job,
                from_env.max_attr_value_len,
                from_env.max_decompressed_bytes,
            ) == (kibibytes(1), kibibytes(2), kibibytes(3))
        );
        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=query-frontend",
            "--target-bytes-per-job=4KiB",
            "--max-attr-value-len=5KiB",
            "--max-decompressed-bytes=6KiB",
        ])
        .unwrap();
        check!(
            (
                from_cli.target_bytes_per_job,
                from_cli.max_attr_value_len,
                from_cli.max_decompressed_bytes,
            ) == (kibibytes(4), kibibytes(5), kibibytes(6))
        );
    }

    #[test]
    fn parses_metrics_generator_optional_spanmetrics_switches() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "metrics-generator",
            "--enable-target-info",
            "--enable-status-message",
            "--enable-messaging-system-latency",
        ])
        .unwrap();

        check!(
            (
                cli.metrics.enable_target_info,
                cli.metrics.enable_status_message,
                cli.metrics.enable_messaging_system_latency,
            ) == (true, true, true)
        );
    }

    #[test]
    fn metrics_generator_poll_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_METRICS_GENERATOR_POLL_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::metrics_generator_poll_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_METRICS_GENERATOR_POLL_BATCH_SIZE", "7")
                    .env("KRABKA_TRACES_METRICS_GENERATOR_POLL_ERROR_BACKOFF", "11ms")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["krabka-traces", "--target=metrics-generator"]).unwrap();
        check!(
            (
                from_env.metrics_generator_poll_batch_size,
                from_env.metrics_generator_poll_error_backoff
            ) == (7, krabka_units::millis(11))
        );
        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=metrics-generator",
            "--metrics-generator-poll-batch-size=13",
            "--metrics-generator-poll-error-backoff=17ms",
        ])
        .unwrap();
        check!(
            (
                from_cli.metrics_generator_poll_batch_size,
                from_cli.metrics_generator_poll_error_backoff
            ) == (13, krabka_units::millis(17))
        );
        for flag in [
            "--metrics-generator-poll-batch-size=0",
            "--metrics-generator-poll-error-backoff=0ms",
        ] {
            check!(
                Cli::try_parse_from(["krabka-traces", "--target=metrics-generator", flag]).is_err()
            );
        }
    }

    #[test]
    fn metrics_generator_config_preserves_file_values_without_cli_overrides() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "metrics-generator"]).unwrap();
        let mut cfg = MetricsGenConfig {
            collection_interval: secs(30),
            max_exemplars_per_series: 5,
            edge_ttl: minutes(1),
            edge_store_max_items: 2_000,
            histogram_buckets_ns: vec![1_000.0, 2_000.0],
            remote_write_url: "http://metrics.example/api/v1/push".into(),
            ..MetricsGenConfig::default()
        };

        apply_metrics_generator_cli_overrides(&mut cfg, &cli);

        check!(
            (
                cfg.collection_interval,
                cfg.max_exemplars_per_series,
                cfg.edge_ttl,
                cfg.edge_store_max_items,
                cfg.histogram_buckets_ns.as_slice(),
                cfg.remote_write_url.as_str(),
            ) == (
                secs(30),
                5,
                minutes(1),
                2_000,
                &[1_000.0, 2_000.0][..],
                "http://metrics.example/api/v1/push",
            )
        );

        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "metrics-generator",
            "--collection-interval-secs",
            "45",
            "--max-exemplars-per-series",
            "2",
            "--edge-ttl-secs",
            "9",
            "--edge-store-max-items",
            "77",
            "--histogram-buckets-ns",
            "500,1000,2500",
            "--remote-write-url",
            "http://override.example/api/v1/push",
        ])
        .unwrap();

        apply_metrics_generator_cli_overrides(&mut cfg, &cli);

        check!(
            (
                cfg.collection_interval,
                cfg.max_exemplars_per_series,
                cfg.edge_ttl,
                cfg.edge_store_max_items,
                cfg.histogram_buckets_ns.as_slice(),
                cfg.remote_write_url.as_str(),
            ) == (
                secs(45),
                2,
                secs(9),
                77,
                &[500.0, 1_000.0, 2_500.0][..],
                "http://override.example/api/v1/push",
            )
        );
    }

    #[tokio::test]
    async fn builds_querier_router_from_defaults() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "querier"]).unwrap();

        assert2::assert!(build_querier_router(&cli).await.is_ok());
    }

    #[tokio::test]
    async fn parses_querier_trace_span_limit() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--max-trace-spans",
            "100",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.max_trace_spans == 100);
        check!(build_querier_router(&cli).await.is_ok());
    }

    #[test]
    fn tag_query_filter_autocomplete_limit_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_TAG_QUERY_FILTER_AUTOCOMPLETE_LIMIT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(
                std::env::current_exe().expect("test executable"),
            )
            .args([
                "--exact",
                "tests::tag_query_filter_autocomplete_limit_reads_environment_and_prefers_cli",
            ])
            .env(CHILD, "1")
            .env("KRABKA_TRACES_TAG_QUERY_FILTER_AUTOCOMPLETE_LIMIT", "7")
            .status()
            .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target=querier"]).unwrap();
        check!(from_env.tag_query_filter_autocomplete_limit == 7);
        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=querier",
            "--tag-query-filter-autocomplete-limit=11",
        ])
        .unwrap();
        check!(from_cli.tag_query_filter_autocomplete_limit == 11);
        check!(
            Cli::try_parse_from([
                "krabka-traces",
                "--target=querier",
                "--tag-query-filter-autocomplete-limit=0",
            ])
            .is_err()
        );
    }

    #[tokio::test]
    async fn parses_querier_search_trace_limit() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--max-search-traces",
            "42",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.max_search_traces == 42);
        check!(build_querier_router(&cli).await.is_ok());
    }

    #[test]
    fn parses_querier_traceql_metric_exemplar_limit() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--max-metric-exemplars",
            "7",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
        assert2::assert!(cli.max_metric_exemplars == 7);
        check!(engine_opts_from_cli(&cli).unwrap().max_exemplars == 7);
    }

    #[test]
    fn traceql_policy_parses_defaults_overrides_and_boundaries() {
        let defaults = Cli::try_parse_from(["krabka-traces", "--target", "querier"]).unwrap();
        check!(engine_opts_from_cli(&defaults).unwrap() == EngineOpts::default());

        let configured = Cli::try_parse_from([
            "krabka-traces",
            "--target=querier",
            "--traceql-default-limit=5",
            "--traceql-default-spans-per-span-set=7",
            "--max-search-traces=11",
            "--max-metric-exemplars=13",
            "--traceql-compare-max-values-per-attr=17",
            "--traceql-histogram-buckets=19ms,23ms",
        ])
        .unwrap();
        check!(
            engine_opts_from_cli(&configured).unwrap()
                == EngineOpts {
                    default_limit: 5,
                    default_spss: 7,
                    max_traces: 11,
                    max_exemplars: 13,
                    compare_max_values_per_attr: 17,
                    histogram_buckets: vec![krabka_units::millis(19), krabka_units::millis(23)],
                }
        );

        for flag in [
            "--traceql-default-limit=0",
            "--traceql-default-spans-per-span-set=0",
            "--max-search-traces=0",
            "--traceql-compare-max-values-per-attr=0",
            "--traceql-histogram-buckets=0ms",
        ] {
            check!(
                Cli::try_parse_from(["krabka-traces", "--target=querier", flag]).is_err(),
                "accepted {flag}"
            );
        }
        let unordered = Cli::try_parse_from([
            "krabka-traces",
            "--target=querier",
            "--traceql-histogram-buckets=23ms,19ms",
        ])
        .unwrap();
        check!(engine_opts_from_cli(&unordered).is_err());
    }

    #[test]
    fn traceql_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_TRACEQL_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::traceql_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_TRACEQL_DEFAULT_LIMIT", "5")
                    .env("KRABKA_TRACES_TRACEQL_DEFAULT_SPANS_PER_SPAN_SET", "7")
                    .env("KRABKA_TRACES_TRACEQL_MAX_TRACES", "11")
                    .env("KRABKA_TRACES_TRACEQL_MAX_EXEMPLARS", "13")
                    .env("KRABKA_TRACES_TRACEQL_COMPARE_MAX_VALUES_PER_ATTR", "17")
                    .env("KRABKA_TRACES_TRACEQL_HISTOGRAM_BUCKETS", "19ms,23ms")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target=querier"]).unwrap();
        check!(engine_opts_from_cli(&from_env).unwrap().default_limit == 5);
        check!(
            engine_opts_from_cli(&from_env).unwrap().histogram_buckets
                == vec![krabka_units::millis(19), krabka_units::millis(23)]
        );

        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=querier",
            "--traceql-default-limit=29",
            "--traceql-histogram-buckets=31ms,37ms",
        ])
        .unwrap();
        check!(engine_opts_from_cli(&from_cli).unwrap().default_limit == 29);
        check!(
            engine_opts_from_cli(&from_cli).unwrap().histogram_buckets
                == vec![krabka_units::millis(31), krabka_units::millis(37)]
        );
    }

    #[test]
    fn parses_distributor_trace_span_limit() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--max-spans-per-trace",
            "42",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.max_spans_per_trace == 42);
    }

    #[test]
    fn parses_distributor_ingest_rate_limit() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--max-ingest-spans-per-second",
            "42",
            "--ingest-rate-burst",
            "7",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Distributor));
        assert2::assert!(cli.max_ingest_spans_per_second == 42);
        assert2::assert!(cli.ingest_rate_burst == 7);
    }

    #[test]
    fn parses_compactor_window() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "compactor",
            "--compaction-start-ns",
            "100",
            "--compaction-end-ns",
            "200",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Compactor));
        assert2::assert!(cli.compaction_start == UnixNano(100));
        assert2::assert!(cli.compaction_end == UnixNano(200));
    }

    #[test]
    fn unix_time_policy_reads_uom_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TRACES_UNIX_TIME_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::unix_time_policy_reads_uom_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_TRACES_COMPACTION_START", "1s")
                    .env("KRABKA_TRACES_COMPACTION_END", "2s")
                    .env("KRABKA_TRACES_LIVE_FRONTIER", "3s")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target=compactor"]).unwrap();
        check!(
            (
                from_env.compaction_start,
                from_env.compaction_end,
                from_env.live_frontier,
            ) == (
                UnixNano(1_000_000_000),
                UnixNano(2_000_000_000),
                Some(UnixNano(3_000_000_000)),
            )
        );
        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=compactor",
            "--compaction-start=4s",
            "--compaction-end=5s",
            "--live-frontier=6s",
        ])
        .unwrap();
        check!(
            (
                from_cli.compaction_start,
                from_cli.compaction_end,
                from_cli.live_frontier,
            ) == (
                UnixNano(4_000_000_000),
                UnixNano(5_000_000_000),
                Some(UnixNano(6_000_000_000)),
            )
        );
    }

    #[test]
    fn parses_object_store_url_and_builds_store() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--object-store-url",
            "memory:///tempo/traces",
        ])
        .unwrap();

        check!(cli.object_store_url == "memory:///tempo/traces");
        let configured = build_object_store(&cli).unwrap();
        assert2::assert!(&configured.root == &Url::parse("memory:///tempo/traces").unwrap());
        assert2::assert!(configured.prefix.to_string() == "tempo/traces".to_string());
        assert2::assert!(
            configured.object_key("index/traces.json")
                == "tempo/traces/index/traces.json".to_string()
        );
        assert2::assert!(
            configured.object_key("traces/tenant-a/block.parquet")
                == "tempo/traces/traces/tenant-a/block.parquet".to_string()
        );
    }

    #[tokio::test]
    async fn parses_query_frontend_options_and_builds_router() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "query-frontend",
            "--querier-url",
            "http://querier-a.example:3200,http://querier-b.example:3200",
            "--live-frontier-ns",
            "60000000000",
            "--query-queue-depth",
            "4",
            "--target-bytes-per-job",
            "4096",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::QueryFrontend));
        assert2::assert!(
            cli.querier_url.as_str()
                == "http://querier-a.example:3200,http://querier-b.example:3200"
        );
        assert2::assert!(cli.live_frontier == Some(UnixNano(60_000_000_000)));
        assert2::assert!(cli.query_queue_depth == 4);
        assert2::assert!(cli.target_bytes_per_job == ByteSize::from_bytes(4096));
        check!(build_query_frontend_router(&cli).await.is_ok());
    }
}

mod alloc;
mod apply_metrics_generator_cli_overrides;
mod build_live_store_router;
mod build_object_store;
mod build_querier_router;
mod build_querier_router_with_live;
mod build_query_frontend_router;
mod build_trace_index_catalog;
mod cli;
mod configured_object_store;
mod engine_opts_from_cli;
mod f64_from_usize;
mod frontend_config_from_cli;
mod indexed_live_source;
mod ingest_rate_from_cli;
mod live_i64_param;
mod live_span_batches;
mod max_trace_size;
mod metrics_flags;
mod parse_client_dispatch_queue_capacity;
mod parse_client_frame_max;
mod parse_consumer_fetch_size;
mod parse_non_negative_time_or_secs;
mod parse_non_negative_whole_byte_size_or_bytes;
mod parse_positive_time_or_millis;
mod parse_positive_time_or_nanos;
mod parse_positive_time_or_nanos_f64;
mod parse_positive_time_or_secs;
mod parse_positive_usize;
mod parse_positive_whole_byte_size;
mod parse_promoted_attr;
mod parse_querier_addrs;
mod parse_scan_concat_max;
mod parse_time_or_legacy_i64;
mod parse_unix_nano;
mod promoted_attrs_from_cli;
mod run;
mod run_block_builder;
mod run_compactor;
mod run_compactor_once;
mod run_distributor;
mod run_live_store;
mod run_metrics_generator;
mod run_querier;
mod run_query_frontend;
mod target;
mod wal_consumer;

#[cfg(all(unix, feature = "heap-profiling"))]
use alloc::ALLOC;

use apply_metrics_generator_cli_overrides::apply_metrics_generator_cli_overrides;
use build_live_store_router::build_live_store_router;
use build_object_store::build_object_store;
#[cfg(test)]
use build_querier_router::build_querier_router;
use build_querier_router_with_live::build_querier_router_with_live;
#[cfg(test)]
use build_query_frontend_router::build_query_frontend_router;
use build_trace_index_catalog::build_trace_index_catalog;
use cli::Cli;
use configured_object_store::ConfiguredObjectStore;
use engine_opts_from_cli::engine_opts_from_cli;
use f64_from_usize::f64_from_usize;
use frontend_config_from_cli::frontend_config_from_cli;
use indexed_live_source::IndexedLiveSource;
use ingest_rate_from_cli::ingest_rate_from_cli;
use live_i64_param::live_i64_param;
use live_span_batches::live_span_batches;
use max_trace_size::max_trace_size;
use metrics_flags::MetricsFlags;
use parse_client_dispatch_queue_capacity::parse_client_dispatch_queue_capacity;
use parse_client_frame_max::parse_client_frame_max;
use parse_consumer_fetch_size::parse_consumer_fetch_size;
use parse_non_negative_time_or_secs::parse_non_negative_time_or_secs;
use parse_non_negative_whole_byte_size_or_bytes::parse_non_negative_whole_byte_size_or_bytes;
use parse_positive_time_or_millis::parse_positive_time_or_millis;
use parse_positive_time_or_nanos::parse_positive_time_or_nanos;
use parse_positive_time_or_nanos_f64::parse_positive_time_or_nanos_f64;
use parse_positive_time_or_secs::parse_positive_time_or_secs;
use parse_positive_usize::parse_positive_usize;
use parse_positive_whole_byte_size::parse_positive_whole_byte_size;
use parse_promoted_attr::parse_promoted_attr;
use parse_querier_addrs::parse_querier_addrs;
use parse_scan_concat_max::parse_scan_concat_max;
use parse_time_or_legacy_i64::parse_time_or_legacy_i64;
use parse_unix_nano::parse_unix_nano;
use promoted_attrs_from_cli::promoted_attrs_from_cli;
use run::run;
use run_block_builder::run_block_builder;
use run_compactor::run_compactor;
use run_compactor_once::run_compactor_once;
use run_distributor::run_distributor;
use run_live_store::run_live_store;
use run_metrics_generator::run_metrics_generator;
use run_querier::run_querier;
use run_query_frontend::run_query_frontend;
use target::Target;
use wal_consumer::wal_consumer;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    // `run` fans out over every role, so its state machine is large; boxing keeps
    // it off the startup task's stack.
    match Box::pin(run(cli)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(2)
        }
    }
}

use std::{net::SocketAddr, process::ExitCode, sync::Arc};

use arc_swap::ArcSwap;
use clap::{ArgAction, Args, Parser, ValueEnum};
use krabka_blockstore::{
    BlockLevel, BlockStore, BlockTimestampUnit, BlockWriter, CompactionPolicy,
    DEFAULT_MAX_BLOCKS_PER_JOB, DEFAULT_MAX_LEVEL, DEFAULT_TARGET_ROWS_PER_BLOCK,
    IndexSnapshotRetain, PromotedSpanAttr, TENANT_HEADER, TenantId, TenantPolicy, TraceIndex,
};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerFetchMaxBytes};
use krabka_client_core::{
    ClientFrameMax, ClientSecurity, ConnectionDispatchQueueCapacity,
    DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_client_producer::Producer;
use krabka_observability::{
    ConfigFileArgs, argv_with_config_file,
    audit::AuditArgs,
    server_security::{
        InternalClient, ServerListener, ServerSecurity, ServerSecurityArgs,
        install_crypto_provider, serve_router,
    },
    wal_client_security::WalClientSecurityArgs,
};
use krabka_telemetry::OtlpConfig;
use krabka_traceql::{EngineOpts, TraceqlEngine};
use krabka_traces::{
    Limits, LiveStore, TRACES_WAL_TOPIC, blockbuilder,
    compactor::{
        compact_once_with_policy, delete_trace_blocks, expire_trace_blocks,
        sweep_orphaned_trace_blocks,
    },
    distributor::{self, DistributorState, KafkaSink},
    frontend::{self, FrontendConfig, TraceIndexCatalog},
    ids::UnixNano,
    limits::OverridesProvider,
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
    use krabka_broker::{Broker, BrokerConfig};
    use krabka_observability::{
        RoleReadiness,
        topic_contract::{TRACES_TOPICS, TopicSettings, provision_topics},
    };
    use krabka_units::{hours, minutes, secs};
    use tower::ServiceExt;

    use super::*;

    // The live-store routes read their principal from the request extensions,
    // where the authentication layer puts it. The tests serve the router
    // behind the unconfigured layer, as `serve_router` does with no flags.
    fn build_live_store_router(
        cli: &Cli,
        live_store: Arc<RwLock<LiveStore>>,
        readiness: RoleReadiness,
    ) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
        super::build_live_store_router(cli, live_store, readiness).map(|router| {
            krabka_observability::server_security::authenticate_requests(
                router,
                &ServerSecurity::default(),
            )
        })
    }

    /// Every listener this binary binds, and not one of them on loopback. A
    /// container that binds loopback is unreachable from outside the pod, and
    /// the symptom is a health check that fails with nothing in the logs.
    /// Tempo defaults its receivers to every interface; so does this.
    #[test]
    fn default_listen_addresses_are_reachable_from_outside_the_container() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "distributor"]).unwrap();

        for (name, addr) in [
            ("listen", &cli.listen),
            ("grpc-listen", &cli.grpc_listen),
            ("otlp-http-listen", &cli.otlp_http_listen),
            ("jaeger-grpc-listen", &cli.jaeger_grpc_listen),
            ("jaeger-compact-listen", &cli.jaeger_compact_listen),
            ("jaeger-http-listen", &cli.jaeger_http_listen),
            ("zipkin-listen", &cli.zipkin_listen),
        ] {
            let parsed: std::net::SocketAddr = addr.parse().expect(name);
            check!(parsed.ip().is_unspecified(), "--{name} defaults to {addr}");
        }
        check!(cli.admin_listen_addr.ip().is_unspecified());
    }

    /// The binary's own flags, out of a file. The generic precedence rules
    /// have their own suite; this one is here because a `Cli` that forgot to
    /// flatten `ConfigFileArgs` would pass every one of those and still
    /// ignore an operator's file.
    #[test]
    fn a_config_file_supplies_this_binary_s_flags() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("krabka.yaml");
        std::fs::write(&path, "target: querier\nlisten: 0.0.0.0:4444\n").unwrap();

        let argv = krabka_observability::argv_with_config_file::<Cli>(vec![
            "krabka-traces".into(),
            "--config.file".into(),
            path.into_os_string(),
        ])
        .unwrap();
        let cli = Cli::parse_from(argv);

        check!(cli.listen == "0.0.0.0:4444");
    }

    /// One vocabulary, one spelling.
    ///
    /// `--target` is what an operator types; `RoleKind::as_str` is what a
    /// readiness gate prints, what a manifest carries, and what the other
    /// three signals' binaries accept for the same stage. Nothing forces the
    /// two to agree: clap derives its names from the variant identifiers and
    /// `RoleKind` spells its own, so a stage renamed on one side and not the
    /// other would give the same role two names, with no error anywhere and
    /// only a deployment that silently does not start to show for it. This is
    /// the check that makes them one name. It goes through clap rather than
    /// reading the source, because clap's rendering is the thing an operator
    /// meets.
    #[test]
    fn every_target_spells_its_role_the_way_the_rest_of_the_stack_does() {
        let mut mismatched = Vec::new();
        for target in Target::value_variants() {
            let typed = target
                .to_possible_value()
                .expect("every --target variant is reachable from the command line")
                .get_name()
                .to_string();
            if typed != target.kind().as_str() {
                mismatched.push((typed.clone(), target.kind().as_str()));
                continue;
            }
            // And the name round-trips: what clap prints is what clap parses.
            let parsed = Cli::try_parse_from(["krabka-traces", "--target", &typed])
                .map(|cli| cli.target)
                .ok();
            check!(parsed == Some(*target), "--target {typed}");
        }
        check!(mismatched.is_empty());
    }

    /// The composite is a target of this binary, not only of the vocabulary.
    #[test]
    fn all_is_a_target_and_names_the_composite_role() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "all"]).unwrap();

        check!(cli.target == Target::All);
        check!(cli.target.kind().is_composite());
        check!(
            Target::value_variants()
                .iter()
                .filter(|target| target.kind().is_composite())
                .count()
                == 1,
            "only one target composes the others"
        );
    }

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

        assert2::assert!(cli.otlp_http_listen.as_str() == "0.0.0.0:4318");
        assert2::assert!(cli.jaeger_grpc_listen.as_str() == "0.0.0.0:14250");
        assert2::assert!(cli.jaeger_http_listen.as_str() == "0.0.0.0:14268");
        assert2::assert!(cli.zipkin_listen.as_str() == "0.0.0.0:9411");
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

    // The three security flag groups parse into their own structs, beside
    // the flags this binary already had.
    #[test]
    fn server_security_audit_and_wal_security_flags_parse() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--server-tls-cert-path",
            "server.pem",
            "--server-tls-key-path",
            "server-key.pem",
            "--auth-credentials-config",
            "credentials.yaml",
            "--internal-client-token-path",
            "internal-token",
            "--audit-topic",
            "krabka-audit",
            "--audit-partition",
            "3",
            "--wal-security-protocol",
            "SASL_SSL",
            "--wal-tls-ca-path",
            "broker-ca.pem",
            "--wal-tls-server-name",
            "broker.internal",
            "--wal-sasl-mechanism",
            "SCRAM-SHA-512",
            "--wal-sasl-username",
            "traces",
            "--wal-sasl-password-path",
            "wal-password",
        ])
        .unwrap();

        check!(
            (
                cli.server_security.server_tls_cert_path.as_deref(),
                cli.server_security.server_tls_key_path.as_deref(),
                cli.server_security.auth_credentials_config.as_deref(),
                cli.server_security.internal_client_token_path.as_deref(),
            ) == (
                Some(std::path::Path::new("server.pem")),
                Some(std::path::Path::new("server-key.pem")),
                Some(std::path::Path::new("credentials.yaml")),
                Some(std::path::Path::new("internal-token")),
            )
        );
        check!(
            cli.audit
                == krabka_observability::audit::AuditArgs {
                    topic: Some("krabka-audit".to_string()),
                    bootstrap: None,
                    partition: krabka_observability::PartitionIndex(3),
                    spool_dir: None,
                    spool_max: krabka_observability::audit::DEFAULT_AUDIT_SPOOL_MAX,
                    queue_capacity: krabka_observability::audit::DEFAULT_AUDIT_QUEUE_CAPACITY,
                    checkpoint_every: krabka_observability::audit::DEFAULT_AUDIT_CHECKPOINT_EVERY,
                    signing_key_path: None,
                    signing_key_id: None,
                }
        );
        check!(
            cli.wal_security
                == WalClientSecurityArgs {
                    wal_security_protocol:
                        krabka_observability::wal_client_security::WalSecurityProtocol::SaslSsl,
                    wal_tls_ca_path: Some("broker-ca.pem".into()),
                    wal_tls_server_name: Some("broker.internal".to_string()),
                    wal_sasl_mechanism: Some(
                        krabka_observability::wal_client_security::WalSaslMechanism::ScramSha512
                    ),
                    wal_sasl_username: Some("traces".to_string()),
                    wal_sasl_password_path: Some("wal-password".into()),
                    ..WalClientSecurityArgs::default()
                }
        );
    }

    // No security flag is the upstream default: plain listeners without
    // authentication, a plain-text broker connection, and no audit trail.
    #[test]
    fn no_security_flag_loads_the_upstream_default() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "all"]).unwrap();

        let server = cli.server_security.load().unwrap();
        check!(
            (
                server.tls_enabled(),
                server.authentication_enabled(),
                server.internal_client().is_configured(),
            ) == (false, false, false)
        );
        check!(cli.wal_security.load().unwrap().is_none());
        check!(!cli.audit.is_enabled());
        check!(require_internal_credential(&server).is_ok());
    }

    // Under `--target all` the frontend and the querier call the roles beside
    // them through listeners that authenticate, so authentication without an
    // internal credential cannot serve a query and refuses to start.
    #[test]
    fn the_all_in_one_refuses_authentication_without_an_internal_credential() {
        let directory = tempfile::tempdir().unwrap();
        let credentials = directory.path().join("credentials.yaml");
        std::fs::write(
            &credentials,
            format!(
                "principals:\n  - name: internal\n    token_sha256: [\"{}\"]\n    tenants: [\"*\"]\n",
                "0".repeat(64)
            ),
        )
        .unwrap();
        let token = directory.path().join("internal-token");
        std::fs::write(&token, "internal-token-value\n").unwrap();
        let credentials = credentials.to_str().unwrap();
        let token = token.to_str().unwrap();

        let cases: [(&str, &[&str], bool); 4] = [
            ("no flag", &[], true),
            (
                "authentication only",
                &["--auth-credentials-config", credentials],
                false,
            ),
            (
                "authentication and an internal token",
                &[
                    "--auth-credentials-config",
                    credentials,
                    "--internal-client-token-path",
                    token,
                ],
                true,
            ),
            (
                "an internal token only",
                &["--internal-client-token-path", token],
                true,
            ),
        ];
        for (case, flags, starts) in cases {
            let mut argv = vec!["krabka-traces", "--target", "all"];
            argv.extend_from_slice(flags);
            let security = Cli::try_parse_from(argv)
                .unwrap()
                .server_security
                .load()
                .unwrap();
            check!(
                require_internal_credential(&security).is_ok() == starts,
                "{case}"
            );
        }
    }

    // One pool, one scheme: the frontend dials every querier with one
    // client, so `https` selects TLS and a list that mixes schemes is refused.
    #[test]
    fn querier_urls_name_one_scheme_for_the_whole_pool() {
        check!(
            parse_querier_addrs("http://querier-a:3200, http://querier-b:3200").unwrap()
                == (
                    krabka_traces::frontend::QuerierScheme::Http,
                    vec!["querier-a:3200".to_string(), "querier-b:3200".to_string()],
                )
        );
        check!(
            parse_querier_addrs("https://querier-a:3200").unwrap()
                == (
                    krabka_traces::frontend::QuerierScheme::Https,
                    vec!["querier-a:3200".to_string()],
                )
        );
        for refused in [
            "http://querier-a:3200,https://querier-b:3200",
            "ftp://querier-a:3200",
            "",
        ] {
            check!(parse_querier_addrs(refused).is_err(), "{refused:?}");
        }
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
        let router = build_live_store_router(&cli, store, RoleReadiness::new()).unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/07070707070707070707070707070707")
                    .header(TENANT_HEADER, "tenant-a")
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
        let router = build_live_store_router(&cli, store, RoleReadiness::new()).unwrap();
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
                row_count: 0,
                level: BlockLevel::INGESTED,
            },
        );
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(index)),
            &InternalClient::default(),
        )
        .unwrap();

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
        let router = build_live_store_router(&cli, store, RoleReadiness::new()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(TraceIndex::new())),
            &InternalClient::default(),
        )
        .unwrap();

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
        let router = build_live_store_router(&cli, store, RoleReadiness::new()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let source = trace_querier::live::RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}")).unwrap(),
            Arc::new(ArcSwap::from_pointee(TraceIndex::new())),
            &InternalClient::default(),
        )
        .unwrap();

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
        let live_router = build_live_store_router(&live_cli, store, RoleReadiness::new()).unwrap();
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
                    .header(TENANT_HEADER, "tenant-a")
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
                row_count: 0,
                level: BlockLevel::INGESTED,
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
                row_count: 0,
                level: BlockLevel::INGESTED,
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

    /// `usize::MAX` is the "no limit" sentinel and converts to the zero the
    /// shared limits read as unlimited. Every other value converts to itself,
    /// which is what separates the sentinel test from its negation: inverted,
    /// it is every ordinary value that collapses to zero.
    #[test]
    fn the_no_limit_sentinel_converts_to_zero_and_nothing_else_does() {
        let limit = u64_limit_from_usize;

        check!(limit(usize::MAX) == 0, "the sentinel means unlimited");
        check!(limit(0) == 0, "and a real zero is already zero");
        check!(limit(1) == 1);
        check!(limit(7) == 7);
        check!(limit(usize::MAX - 1) == u64::try_from(usize::MAX - 1).expect("fits in u64"));
    }

    /// Every limit flag reaches the provider's defaults, and each one lands in
    /// its own field. The whole struct is compared, so a flag wired to the
    /// neighbouring field, or a field left on the compiled default, fails here.
    /// `max_traces_per_search` and `max_search_duration` had no route from the
    /// command line at all: the distributor projected its own limit type onto
    /// the shared one and pinned those two to `Limits::default()`.
    #[test]
    fn every_limit_flag_reaches_the_provider_defaults() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--max-ingest-spans-per-second",
            "11",
            "--ingest-rate-burst",
            "22",
            "--max-spans-per-request",
            "33",
            "--max-traces-per-search",
            "44",
            "--max-spans-per-trace",
            "55",
            "--max-attr-value-len",
            "66",
            "--max-search-duration",
            "77s",
            "--block-retention",
            "88h",
        ])
        .unwrap();

        check!(
            limits_from_cli(&cli)
                == Limits {
                    ingestion_rate: krabka_units::per_sec(11),
                    ingestion_burst_spans: 22,
                    max_spans_per_request: 33,
                    max_traces_per_search: 44,
                    max_spans_per_trace: 55,
                    max_attribute: krabka_units::bytes(66),
                    max_search_duration: secs(77),
                    block_retention: hours(88),
                }
        );
    }

    #[test]
    fn parses_traces_limits_overrides_config() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--traces-limits-overrides-config",
            "overrides.yaml",
        ])
        .unwrap();

        check!(
            cli.traces_limits_overrides_config.as_deref()
                == Some(std::path::Path::new("overrides.yaml"))
        );
    }

    /// With no file, every tenant gets the flags. This is why the loader hands
    /// back a provider and not an `Option`: a role with no overrides file still
    /// resolves a tenant through the one provider it built.
    #[test]
    fn absent_overrides_file_leaves_every_tenant_on_the_flags() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "distributor",
            "--max-spans-per-request",
            "9",
        ])
        .unwrap();

        let overrides = load_traces_limits_overrides_config(None, limits_from_cli(&cli)).unwrap();

        check!(*overrides.for_tenant("tenant-a") == limits_from_cli(&cli));
        check!(overrides.for_tenant("tenant-a").max_spans_per_request == 9);
    }

    /// A listed tenant takes the keys of its entry and keeps the flags for the
    /// rest. The whole struct is compared, so a merge that reset an unnamed
    /// field to the compiled default fails here rather than in production.
    #[test]
    fn loads_traces_limits_overrides_config_from_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overrides.yaml");
        std::fs::write(
            &path,
            r"
overrides:
  tenant-a:
    max_traces_per_search: 3
    max_search_duration_secs: 30
    max_spans_per_request: 4
    block_retention: 12h
",
        )
        .unwrap();
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--max-spans-per-trace",
            "500",
            "--traces-limits-overrides-config",
            path.to_str().unwrap(),
        ])
        .unwrap();

        let overrides = load_traces_limits_overrides_config(
            cli.traces_limits_overrides_config.as_deref(),
            limits_from_cli(&cli),
        )
        .unwrap();

        check!(
            *overrides.for_tenant("tenant-a")
                == Limits {
                    ingestion_rate: <Frequency as FrequencyExt>::ZERO,
                    ingestion_burst_spans: 0,
                    max_spans_per_request: 4,
                    max_traces_per_search: 3,
                    max_spans_per_trace: 500,
                    max_attribute: krabka_units::kibibytes(64),
                    max_search_duration: secs(30),
                    block_retention: hours(12),
                }
        );
        // A tenant the file does not name keeps the flags, not the compiled
        // defaults: `--max-spans-per-trace` reaches it too.
        check!(*overrides.for_tenant("tenant-b") == limits_from_cli(&cli));
        check!(overrides.for_tenant("tenant-b").max_spans_per_trace == 500);
    }

    /// End to end through the flag: a file named on the command line reaches
    /// the querier's read gate and changes the answer one tenant gets.
    #[tokio::test]
    async fn an_overrides_file_reaches_the_querier_read_gate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overrides.yaml");
        std::fs::write(
            &path,
            r"
overrides:
  tenant-tight:
    max_traces_per_search: 1
",
        )
        .unwrap();
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "querier",
            "--traces-limits-overrides-config",
            path.to_str().unwrap(),
        ])
        .unwrap();
        let router = build_querier_router(&cli).await.unwrap();
        let search = |tenant: &'static str| {
            let router = router.clone();
            async move {
                router
                    .oneshot(
                        Request::builder()
                            .uri("/api/search?q=%7B%7D&start=0&end=1&limit=2")
                            .header(TENANT_HEADER, tenant)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .status()
            }
        };

        check!(search("tenant-tight").await == HttpStatusCode::BAD_REQUEST);
        check!(search("tenant-loose").await == HttpStatusCode::OK);
    }

    /// The two cardinality caps reach the generator's config from the command
    /// line, and a file value survives when the flag is absent. A flag that
    /// parsed but was never applied would leave an operator with a map as wide
    /// as the process default, and nothing to show for the number typed.
    #[test]
    fn cardinality_cap_flags_reach_the_metrics_generator_config() {
        let mut cfg = MetricsGenConfig {
            max_active_series: 2_000,
            max_tenants: 33,
            ..MetricsGenConfig::default()
        };
        let without = Cli::try_parse_from(["krabka-traces", "--target", "metrics-generator"])
            .expect("no cap flags parses");

        apply_metrics_generator_cli_overrides(&mut cfg, &without);
        check!((cfg.max_active_series, cfg.max_tenants) == (2_000, 33));

        let with = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "metrics-generator",
            "--max-active-series",
            "77",
            "--metrics-generator-max-tenants",
            "5",
        ])
        .expect("cap flags parse");

        apply_metrics_generator_cli_overrides(&mut cfg, &with);
        check!((cfg.max_active_series, cfg.max_tenants) == (77, 5));
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

    /// Compaction is configured by a policy, not by a time window: what merges
    /// with what follows from the levels and time ranges the index already
    /// records.
    #[test]
    fn parses_the_compaction_policy() {
        let cli = Cli::try_parse_from([
            "krabka-traces",
            "--target",
            "compactor",
            "--compaction-max-blocks-per-job",
            "4",
            "--compaction-target-rows",
            "250000",
            "--compaction-max-level",
            "3",
            "--compaction-level-window",
            "30m",
            "--compaction-interval",
            "90s",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Compactor));
        let policy = compaction_policy_from_cli(&cli);
        check!(policy.max_blocks_per_job() == 4);
        check!(policy.target_rows_per_block() == 250_000);
        check!(policy.max_level() == BlockLevel(3));
        check!(policy.window_ticks_for(BlockLevel(0)) == 1_800_000_000_000);
        check!(policy.window_ticks_for(BlockLevel(1)) == 3_600_000_000_000);
        check!(cli.compaction_interval == secs(90));
    }

    /// The defaults have to make a usable ladder on their own, because the
    /// compactor now runs unattended.
    #[test]
    fn the_default_compaction_policy_is_a_usable_ladder() {
        let cli = Cli::try_parse_from(["krabka-traces", "--target", "compactor"]).unwrap();
        let policy = compaction_policy_from_cli(&cli);
        check!(policy.max_blocks_per_job() == DEFAULT_MAX_BLOCKS_PER_JOB);
        check!(policy.target_rows_per_block() == DEFAULT_TARGET_ROWS_PER_BLOCK);
        check!(policy.max_level() == DEFAULT_MAX_LEVEL);
        check!(policy.window_ticks_for(BlockLevel(0)) == 7_200_000_000_000);
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
                    .env("KRABKA_TRACES_COMPACTION_INTERVAL", "1s")
                    .env("KRABKA_TRACES_COMPACTION_LEVEL_WINDOW", "2s")
                    .env("KRABKA_TRACES_LIVE_FRONTIER", "3s")
                    .status()
                    .expect("child test");
            check!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-traces", "--target=compactor"]).unwrap();
        check!(
            (
                from_env.compaction_interval,
                from_env.compaction_level_window,
                from_env.live_frontier,
            ) == (secs(1), secs(2), Some(UnixNano(3_000_000_000)))
        );
        let from_cli = Cli::try_parse_from([
            "krabka-traces",
            "--target=compactor",
            "--compaction-interval=4s",
            "--compaction-level-window=5s",
            "--live-frontier=6s",
        ])
        .unwrap();
        check!(
            (
                from_cli.compaction_interval,
                from_cli.compaction_level_window,
                from_cli.live_frontier,
            ) == (secs(4), secs(5), Some(UnixNano(6_000_000_000)))
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
        let configured =
            build_object_store(&cli, krabka_blockstore::ObjectStoreMetrics::unregistered())
                .unwrap();
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

    /// The contract has to stop a start, not merely describe one.
    ///
    /// The traces WAL is keyed by trace id, so a role that ran against a topic
    /// the deployment never provisioned -- or provisioned at a different
    /// partition count -- would scatter one trace's spans with nothing on the
    /// wire to say so. The four roles that open a WAL client must refuse; the
    /// three that never reach a broker must be untouched by the same fault.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_missing_wal_topic_stops_the_roles_that_use_it_and_no_others() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();

        // The querier appears twice: it tails the WAL only with an embedded
        // live store, and reaches no broker without one.
        let roles: [(&[&str], bool); 9] = [
            // `all` runs every WAL client this binary has, so it refuses for
            // the same reason all four of them do -- once, not four times.
            (&["--target", "all"], true),
            (&["--target", "distributor"], true),
            (&["--target", "block-builder"], true),
            (&["--target", "live-store"], true),
            (&["--target", "metrics-generator"], true),
            (&["--target", "querier", "--querier-live-store"], true),
            (&["--target", "querier"], false),
            (&["--target", "query-frontend"], false),
            (&["--target", "compactor"], false),
        ];

        for (args, refuses) in roles {
            let cli = cli_with(args, &bootstrap);
            let outcome = require_role_topics(&cli, None).await;
            check!(outcome.is_err() == refuses, "{args:?} before provisioning");
            if let Err(error) = outcome {
                check!(error.to_string().contains(TRACES_WAL_TOPIC), "{args:?}");
            }
        }

        create_traces_wal_topic(&bootstrap).await;

        for (args, _) in roles {
            check!(
                require_role_topics(&cli_with(args, &bootstrap), None)
                    .await
                    .is_ok(),
                "{args:?} after provisioning"
            );
        }
    }

    fn cli_with(role_flags: &[&str], bootstrap: &str) -> Cli {
        let mut argv = vec!["krabka-traces"];
        argv.extend_from_slice(role_flags);
        argv.extend_from_slice(&["--bootstrap", bootstrap]);
        Cli::try_parse_from(argv).expect("cli")
    }

    /// Provisions the traces WAL topic the way the deployment step does,
    /// through the same call `krabka-o11y-bootstrap` makes.
    async fn create_traces_wal_topic(bootstrap: &str) {
        provision_topics(
            bootstrap,
            &TRACES_TOPICS,
            &TopicSettings::single_broker(),
            None,
        )
        .await
        .expect("provision the traces WAL topic");
    }
}

/// One compaction pass, in the order the role runs its parts. The module is in
/// the binary crate because `run_compactor_once` is the role's own step and no
/// integration test can reach it.
#[cfg(test)]
mod a_compaction_pass_deletes_what_it_retires;

/// `--target all` in one child process, driven only through its ports. The
/// module is in the binary crate so the child can call `run` on a real `Cli`,
/// and so it builds under Bazel as well as Cargo -- an integration test would
/// have needed `CARGO_BIN_EXE_krabka-traces`, which only Cargo defines.
#[cfg(test)]
mod all_in_one_serves_ingest_and_query;

/// The compactor role, started and stopped as `--target compactor` and as the
/// `--target all` stage. The module is in the binary crate because both of
/// those call `run_compactor` itself, which no integration test can reach.
#[cfg(test)]
mod the_compactor_runs_under_supervision;

mod all_role_context;
mod all_role_stage;
mod all_role_stages;
mod alloc;
mod apply_metrics_generator_cli_overrides;
mod block_store_gates;
mod build_live_store_router;
mod build_object_store;
mod build_querier_router;
mod build_querier_router_with_live;
mod build_query_frontend_router;
mod build_trace_index_catalog;
mod cli;
mod compaction_loop;
mod compaction_policy_from_cli;
mod configured_object_store;
mod engine_opts_from_cli;
mod f64_from_usize;
mod frontend_config_from_cli;
mod indexed_live_source;
mod ingest_rate_from_cli;
mod limits_from_cli;
mod live_i64_param;
mod live_span_batches;
mod load_traces_limits_overrides_config;
mod log_role_outcome;
mod max_trace_size;
mod metrics_flags;
mod now_unix_nanos;
mod parse_client_dispatch_queue_capacity;
mod parse_client_frame_max;
mod parse_consumer_fetch_size;
mod parse_min_two_usize;
mod parse_non_negative_time_or_secs;
mod parse_non_negative_whole_byte_size_or_bytes;
mod parse_positive_time_or_millis;
mod parse_positive_time_or_nanos;
mod parse_positive_time_or_nanos_f64;
mod parse_positive_time_or_secs;
mod parse_positive_u32;
mod parse_positive_usize;
mod parse_positive_whole_byte_size;
mod parse_promoted_attr;
mod parse_querier_addrs;
mod parse_scan_concat_max;
mod parse_time_or_legacy_i64;
mod parse_unix_nano;
mod process_security;
mod promoted_attrs_from_cli;
mod require_internal_credential;
mod require_role_topics;
mod run;
mod run_all;
mod run_all_query_frontend;
mod run_block_builder;
mod run_compactor;
mod run_compactor_once;
mod run_distributor;
mod run_live_store;
mod run_metrics_generator;
mod run_querier;
mod run_query_frontend;
mod shared_object_store;
mod target;
mod u64_limit_from_usize;
mod wal_consumer;

// `alloc` deliberately has no `use` line. `#[global_allocator]` registers
// the static by attribute, so naming it here imports something nothing
// reads -- which is a warning, not a link to the allocator.

use all_role_context::AllRoleContext;
use all_role_stage::{AllRoleStage, all_role_stage};
use all_role_stages::all_role_stages;
use apply_metrics_generator_cli_overrides::apply_metrics_generator_cli_overrides;
use block_store_gates::BlockStoreGates;
use build_live_store_router::build_live_store_router;
use build_object_store::build_object_store;
#[cfg(test)]
use build_querier_router::build_querier_router;
use build_querier_router_with_live::build_querier_router_with_live;
#[cfg(test)]
use build_query_frontend_router::build_query_frontend_router;
use build_trace_index_catalog::build_trace_index_catalog;
use cli::Cli;
use compaction_loop::compaction_loop;
use compaction_policy_from_cli::compaction_policy_from_cli;
use configured_object_store::ConfiguredObjectStore;
use engine_opts_from_cli::engine_opts_from_cli;
use f64_from_usize::f64_from_usize;
use frontend_config_from_cli::frontend_config_from_cli;
use indexed_live_source::IndexedLiveSource;
use ingest_rate_from_cli::ingest_rate_from_cli;
use limits_from_cli::limits_from_cli;
use live_i64_param::live_i64_param;
use live_span_batches::live_span_batches;
use load_traces_limits_overrides_config::load_traces_limits_overrides_config;
use log_role_outcome::log_role_outcome;
use max_trace_size::max_trace_size;
use metrics_flags::MetricsFlags;
use now_unix_nanos::now_unix_nanos;
use parse_client_dispatch_queue_capacity::parse_client_dispatch_queue_capacity;
use parse_client_frame_max::parse_client_frame_max;
use parse_consumer_fetch_size::parse_consumer_fetch_size;
use parse_min_two_usize::parse_min_two_usize;
use parse_non_negative_time_or_secs::parse_non_negative_time_or_secs;
use parse_non_negative_whole_byte_size_or_bytes::parse_non_negative_whole_byte_size_or_bytes;
use parse_positive_time_or_millis::parse_positive_time_or_millis;
use parse_positive_time_or_nanos::parse_positive_time_or_nanos;
use parse_positive_time_or_nanos_f64::parse_positive_time_or_nanos_f64;
use parse_positive_time_or_secs::parse_positive_time_or_secs;
use parse_positive_u32::parse_positive_u32;
use parse_positive_usize::parse_positive_usize;
use parse_positive_whole_byte_size::parse_positive_whole_byte_size;
use parse_promoted_attr::parse_promoted_attr;
use parse_querier_addrs::parse_querier_addrs;
use parse_scan_concat_max::parse_scan_concat_max;
use parse_time_or_legacy_i64::parse_time_or_legacy_i64;
use parse_unix_nano::parse_unix_nano;
use process_security::ProcessSecurity;
use promoted_attrs_from_cli::promoted_attrs_from_cli;
use require_internal_credential::require_internal_credential;
use require_role_topics::require_role_topics;
use run::run;
use run_all::run_all;
use run_all_query_frontend::run_all_query_frontend;
use run_block_builder::run_block_builder;
use run_compactor::run_compactor;
use run_compactor_once::run_compactor_once;
use run_distributor::run_distributor;
use run_live_store::run_live_store;
use run_metrics_generator::run_metrics_generator;
use run_querier::run_querier;
use run_query_frontend::run_query_frontend;
use shared_object_store::SharedObjectStore;
use target::Target;
use u64_limit_from_usize::u64_limit_from_usize;
use wal_consumer::wal_consumer;

#[tokio::main]
async fn main() -> ExitCode {
    // First, before anything can open a TLS connection: the Kafka client,
    // `reqwest` and tonic all panic without a process-wide rustls provider.
    install_crypto_provider();
    let argv = match argv_with_config_file::<Cli>(std::env::args_os()) {
        Ok(argv) => argv,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let cli = Cli::parse_from(argv);
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

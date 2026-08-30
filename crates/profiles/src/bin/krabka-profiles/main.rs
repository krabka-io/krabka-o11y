use std::{
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};

use clap::{Parser, ValueEnum};
use krabka_blockstore::{IndexSnapshotRetain, ProfileIndex};
use krabka_client_consumer::ConsumerFetchMaxBytes;
use krabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_client_producer::Producer;
use krabka_pprof::{DebuginfodConfig, UnionProfileStore};
use krabka_profiles::{
    blockbuilder::BlockBuilderConfig,
    cold_store::ColdProfileStore,
    compactor::{DownsamplePolicy, compact_once_with_policy},
    distributor::{DistributorState, KafkaSink, serve_supervised},
    hot_store::{RetentionConfig, WalTailProfileStore},
    ingest::{RelabelConfig, TenantLimitConfig},
    limits::{Limits, OverridesProvider},
    metrics::ServiceMetrics,
    query::{QuerierState, serve_supervised as serve_querier},
    query_frontend::FrontendConfig,
};
use krabka_telemetry::OtlpConfig;
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    fmt::Human as _,
    parse,
};
#[cfg(test)]
use krabka_units::{mebibytes, secs};
use object_store::{ObjectStore, path::Path as ObjectPath};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use std::sync::{Mutex as StdMutex, OnceLock};

    use assert2::{assert, check};
    use clap::{CommandFactory, Parser};
    use krabka_units::{bytes, per_sec};

    use super::*;

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults = Cli::try_parse_from(["krabka-profiles", "--target", "querier"]).unwrap();
        assert!(defaults.client_dispatch_queue_capacity == 64);
        assert!(defaults.client_frame_max == mebibytes(100));

        let custom = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
        ])
        .unwrap();
        assert!(custom.client_dispatch_queue_capacity == 7);
        assert!(custom.client_frame_max == krabka_units::kibibytes(32));

        for args in [
            vec![
                "krabka-profiles",
                "--target",
                "querier",
                "--client-dispatch-queue-capacity",
                "0",
            ],
            vec![
                "krabka-profiles",
                "--target",
                "querier",
                "--client-frame-max",
                "101MiB",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_PROFILES_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_PROFILES_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("KRABKA_PROFILES_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-profiles", "--target", "querier"]).unwrap();
        assert!(from_env.client_dispatch_queue_capacity == 7);
        assert!(from_env.client_frame_max == krabka_units::kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ])
        .unwrap();
        assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert!(from_cli.client_frame_max == krabka_units::kibibytes(64));
    }

    static ENV_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    const DEBUGINFOD_ENV: [(&str, Option<&str>); 4] = [
        ("KRABKA_PROFILES_DEBUGINFOD_URLS", None),
        ("KRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE", None),
        ("KRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT", None),
        ("KRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT", None),
    ];

    #[test]
    fn debuginfod_config_preserves_defaults_and_accepts_cli_units() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_vars(DEBUGINFOD_ENV, || {
            let defaults = Cli::try_parse_from(["krabka-profiles", "--target", "querier"]).unwrap();
            assert!(debuginfod_config(&defaults).unwrap() == DebuginfodConfig::default());

            let custom = Cli::try_parse_from([
                "krabka-profiles",
                "--target",
                "querier",
                "--debuginfod-max-artifact-size",
                "64MiB",
                "--debuginfod-connect-timeout",
                "250ms",
                "--debuginfod-request-timeout",
                "3s",
            ])
            .unwrap();
            let config = debuginfod_config(&custom).unwrap();
            assert!(config.max_artifact_size() == mebibytes(64));
            assert!(config.connect_timeout() == krabka_units::millis(250));
            assert!(config.request_timeout() == secs(3));
        });
    }

    #[test]
    fn debuginfod_config_reads_environment() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_vars(
            [
                (
                    "KRABKA_PROFILES_DEBUGINFOD_URLS",
                    Some("http://one.example,http://two.example"),
                ),
                (
                    "KRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE",
                    Some("32MiB"),
                ),
                ("KRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT", Some("500ms")),
                ("KRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT", Some("4s")),
            ],
            || {
                let cli =
                    Cli::try_parse_from(["krabka-profiles", "--target", "symbolizer"]).unwrap();
                let config = debuginfod_config(&cli).unwrap();
                assert!(
                    cli.debuginfod_urls
                        == vec![
                            "http://one.example".to_string(),
                            "http://two.example".to_string()
                        ]
                );
                assert!(config.max_artifact_size() == mebibytes(32));
                assert!(config.connect_timeout() == krabka_units::millis(500));
                assert!(config.request_timeout() == secs(4));
            },
        );
    }

    #[test]
    fn debuginfod_config_rejects_connect_timeout_beyond_request_timeout() {
        let cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "symbolizer",
            "--debuginfod-connect-timeout",
            "5s",
            "--debuginfod-request-timeout",
            "4s",
        ])
        .unwrap();

        assert!(debuginfod_config(&cli).is_err());
    }

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["krabka-profiles", "--target", "distributor"]).unwrap();

        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn runtime_policy_preserves_defaults_and_accepts_units() {
        let defaults = Cli::try_parse_from(["krabka-profiles", "--target", "querier"]).unwrap();
        assert!(defaults.distributor_request_max == mebibytes(16));
        assert!(defaults.distributor_max_tracked_tenants == 4096);
        assert!(defaults.legacy_max_nodes == 500_000);
        assert!(defaults.legacy_max_path_bytes == mebibytes(64));
        assert!(defaults.legacy_max_trie_depth == 4096);
        assert!(defaults.index_refresh_interval == secs(15));
        assert!(defaults.hot_store_max_age == krabka_units::hours(6));
        assert!(defaults.hot_store_max_records == 1_000_000);
        assert!(defaults.heatmap_value_buckets == 32);
        assert!(defaults.heatmap_time_buckets_max == 4096);
        assert!(defaults.query_frontend_shard_width == krabka_units::minutes(15));

        let custom = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "querier",
            "--distributor-request-max",
            "2MiB",
            "--distributor-max-tracked-tenants",
            "32",
            "--legacy-max-nodes",
            "100",
            "--legacy-max-path-bytes",
            "1MiB",
            "--legacy-max-trie-depth",
            "64",
            "--index-refresh-interval",
            "2s",
            "--hot-store-max-age",
            "30m",
            "--hot-store-max-records",
            "500",
            "--heatmap-value-buckets",
            "16",
            "--heatmap-time-buckets-max",
            "256",
            "--query-frontend-shard-width",
            "1m",
            "--block-builder-flush-max-age",
            "3s",
            "--compactor-downsample-resolution",
            "5m",
        ])
        .unwrap();
        assert!(custom.distributor_request_max == mebibytes(2));
        assert!(custom.distributor_max_tracked_tenants == 32);
        assert!(custom.legacy_max_nodes == 100);
        assert!(custom.legacy_max_path_bytes == mebibytes(1));
        assert!(custom.legacy_max_trie_depth == 64);
        assert!(custom.index_refresh_interval == secs(2));
        assert!(custom.hot_store_max_age == krabka_units::minutes(30));
        assert!(custom.hot_store_max_records == 500);
        assert!(custom.heatmap_value_buckets == 16);
        assert!(custom.heatmap_time_buckets_max == 256);
        assert!(custom.query_frontend_shard_width == krabka_units::minutes(1));
        assert!(custom.block_builder_flush_max_age == secs(3));
        assert!(custom.compactor_downsample_resolution == Some(krabka_units::minutes(5)));
    }

    #[test]
    fn runtime_policy_rejects_zero_and_invalid_counts() {
        for (flag, invalid) in [
            ("--distributor-request-max", "0B"),
            ("--distributor-max-tracked-tenants", "0"),
            ("--legacy-max-nodes", "0"),
            ("--legacy-max-path-bytes", "0B"),
            ("--legacy-max-trie-depth", "0"),
            ("--index-refresh-interval", "0s"),
            ("--hot-store-max-age", "0s"),
            ("--hot-store-max-records", "0"),
            ("--heatmap-value-buckets", "0"),
            ("--heatmap-time-buckets-max", "0"),
            ("--query-frontend-shard-width", "0"),
            ("--block-builder-flush-records", "0"),
            ("--block-builder-flush-max-age", "0"),
            ("--compactor-max-blocks-per-job", "1"),
            ("--compactor-downsample-resolution", "0"),
        ] {
            assert!(
                Cli::try_parse_from(["krabka-profiles", "--target", "querier", flag, invalid])
                    .is_err(),
                "{flag} should reject {invalid:?}"
            );
        }
    }

    #[test]
    fn deployment_identity_preserves_defaults_and_rejects_empty_values() {
        let defaults = Cli::try_parse_from(["krabka-profiles", "--target", "querier"]).unwrap();
        assert!(defaults.wal_topic == krabka_profiles::PROFILES_WAL_TOPIC);
        assert!(defaults.block_builder_group_id == "krabka-profiles-block-builder");
        assert!(defaults.index_object_key == "index/profiles.json");

        let custom = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "querier",
            "--wal-topic",
            "profiles-a",
            "--block-builder-group-id",
            "builders-a",
            "--index-object-key",
            "indexes/a.json",
        ])
        .unwrap();
        assert!(custom.wal_topic == "profiles-a");
        assert!(custom.block_builder_group_id == "builders-a");
        assert!(custom.index_object_key == "indexes/a.json");

        for flag in [
            "--wal-topic",
            "--block-builder-group-id",
            "--index-object-key",
            "--query-wal-tail-group-id",
        ] {
            assert!(
                Cli::try_parse_from(["krabka-profiles", "--target", "querier", flag, ""]).is_err(),
                "{flag} should reject an empty value"
            );
        }
    }

    #[test]
    fn runtime_policy_reads_environment_and_cli_wins() {
        const CHILD: &str = "KRABKA_PROFILES_RUNTIME_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::runtime_policy_reads_environment_and_cli_wins",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_PROFILES_DISTRIBUTOR_REQUEST_MAX", "2MiB")
                    .env("KRABKA_PROFILES_DISTRIBUTOR_MAX_TRACKED_TENANTS", "32")
                    .env("KRABKA_PROFILES_INDEX_REFRESH_INTERVAL", "2s")
                    .env("KRABKA_PROFILES_HOT_STORE_MAX_AGE", "30m")
                    .env("KRABKA_PROFILES_HOT_STORE_MAX_RECORDS", "500")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-profiles", "--target", "querier"]).unwrap();
        assert!(from_env.distributor_request_max == mebibytes(2));
        assert!(from_env.distributor_max_tracked_tenants == 32);
        assert!(from_env.index_refresh_interval == secs(2));
        assert!(from_env.hot_store_max_age == krabka_units::minutes(30));
        assert!(from_env.hot_store_max_records == 500);

        let from_cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "querier",
            "--distributor-request-max",
            "3MiB",
            "--distributor-max-tracked-tenants",
            "64",
        ])
        .unwrap();
        assert!(from_cli.distributor_request_max == mebibytes(3));
        assert!(from_cli.distributor_max_tracked_tenants == 64);
    }

    #[test]
    fn every_process_argument_has_an_environment_binding() {
        let command = Cli::command();
        let missing = command
            .get_arguments()
            .filter(|argument| argument.get_long().is_some() && argument.get_env().is_none())
            .filter_map(|argument| argument.get_long().map(str::to_owned))
            .collect::<Vec<_>>();

        assert!(
            missing.is_empty(),
            "arguments without env bindings: {missing:?}"
        );
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["krabka-profiles", "--target", "block-builder"]).unwrap();

        assert!(matches!(cli.target, Target::BlockBuilder));
    }

    #[test]
    fn parses_block_builder_flush_options() {
        let cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "block-builder",
            "--block-builder-flush-records",
            "4096",
            "--block-builder-flush-max-age-ms",
            "60000",
        ])
        .unwrap();

        assert!(cli.block_builder_flush_records == 4096);
        assert!(cli.block_builder_flush_max_age == krabka_units::minutes(1));
    }

    #[test]
    fn index_snapshot_policy_defaults_and_rejects_invalid_values() {
        let cli = Cli::try_parse_from(["krabka-profiles", "--target", "block-builder"]).unwrap();
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
                        "krabka-profiles",
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
                    "krabka-profiles",
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
        const CHILD: &str = "KRABKA_PROFILES_INDEX_SNAPSHOT_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::index_snapshot_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_PROFILES_INDEX_SNAPSHOT_MAX", "1KiB")
                    .env("KRABKA_PROFILES_INDEX_SNAPSHOT_RETAIN", "3")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["krabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.index_snapshot_max.bytes_u64(), 1024);
        assert_eq!(from_env.index_snapshot_retain.into_value(), 3);

        let from_cli = Cli::try_parse_from([
            "krabka-profiles",
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
    fn wal_fetch_limits_preserve_defaults_and_reject_invalid_values() {
        let cli = Cli::try_parse_from(["krabka-profiles", "--target", "block-builder"]).unwrap();
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
                Cli::try_parse_from([
                    "krabka-profiles",
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

    #[test]
    fn wal_fetch_limits_read_environment_and_prefer_cli() {
        const CHILD: &str = "KRABKA_PROFILES_WAL_FETCH_LIMITS_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::wal_fetch_limits_read_environment_and_prefer_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_PROFILES_WAL_FETCH_MAX", "1KiB")
                    .env("KRABKA_PROFILES_WAL_FETCH_PARTITION_MAX", "256B")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["krabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.wal_fetch_max.bytes_i32(), 1024);
        assert_eq!(from_env.wal_fetch_partition_max.bytes_i32(), 256);

        let from_cli = Cli::try_parse_from([
            "krabka-profiles",
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
    fn wal_poll_timeout_preserves_default_and_accepts_units() {
        let defaults =
            Cli::try_parse_from(["krabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(defaults.wal_poll_timeout, krabka_units::millis(500));

        let overridden = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "querier",
            "--wal-poll-timeout",
            "2s",
        ])
        .unwrap();
        assert_eq!(overridden.wal_poll_timeout, krabka_units::secs(2));

        for invalid in ["0", "1", "1KiB"] {
            assert!(
                Cli::try_parse_from([
                    "krabka-profiles",
                    "--target",
                    "query-frontend",
                    "--wal-poll-timeout",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn wal_poll_timeout_reads_environment_and_cli_wins() {
        const CHILD: &str = "KRABKA_PROFILES_WAL_POLL_TIMEOUT_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::wal_poll_timeout_reads_environment_and_cli_wins",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_PROFILES_WAL_POLL_TIMEOUT", "750ms")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["krabka-profiles", "--target", "block-builder"]).unwrap();
        assert_eq!(from_env.wal_poll_timeout, krabka_units::millis(750));

        let from_cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "query-frontend",
            "--wal-poll-timeout",
            "1s",
        ])
        .unwrap();
        assert_eq!(from_cli.wal_poll_timeout, krabka_units::secs(1));
    }

    #[test]
    fn parses_querier_target() {
        let cli = Cli::try_parse_from(["krabka-profiles", "--target", "querier"]).unwrap();

        assert!(matches!(cli.target, Target::Querier));
    }

    #[test]
    fn parses_query_frontend_target_and_shard_width() {
        let cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "query-frontend",
            "--query-frontend-shard-ms",
            "30000",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::QueryFrontend));
        assert!(cli.query_frontend_shard_width == krabka_units::secs(30));
    }

    #[test]
    fn parses_query_wal_tail_group_id() {
        let cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "querier",
            "--query-wal-tail-group-id",
            "profiles-tail-a",
        ])
        .unwrap();

        assert!(cli.query_wal_tail_group_id == "profiles-tail-a");
    }

    #[test]
    fn parses_profiles_limits_overrides_config() {
        let cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "query-frontend",
            "--profiles-limits-overrides-config",
            "overrides.yaml",
        ])
        .unwrap();

        assert!(
            cli.profiles_limits_overrides_config.as_deref() == Some(Path::new("overrides.yaml"))
        );
    }

    #[test]
    fn parses_distributor_profiles_limits_overrides_config() {
        let cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "distributor",
            "--profiles-limits-overrides-config",
            "overrides.yaml",
        ])
        .unwrap();

        assert!(
            cli.profiles_limits_overrides_config.as_deref() == Some(Path::new("overrides.yaml"))
        );
    }

    #[test]
    fn parses_compactor_max_blocks_per_job() {
        let cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "compactor",
            "--compactor-max-blocks-per-job",
            "3",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::Compactor));
        assert!(cli.compactor_max_blocks_per_job == 3);
    }

    #[test]
    fn parses_compactor_downsample_resolution() {
        let cli = Cli::try_parse_from([
            "krabka-profiles",
            "--target",
            "compactor",
            "--compactor-downsample-resolution-ns",
            "60000000000",
        ])
        .unwrap();

        assert!(cli.compactor_downsample_resolution == Some(krabka_units::minutes(1)));
    }

    #[test]
    fn debuginfod_urls_default_is_empty() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_var("KRABKA_PROFILES_DEBUGINFOD_URLS", None::<&str>, || {
            // Security default: no outbound debuginfod egress unless the operator
            // explicitly opts in. The list must be empty when the flag is absent.
            let cli = Cli::try_parse_from(["krabka-profiles", "--target", "querier"]).unwrap();

            assert!(cli.debuginfod_urls.is_empty());
        });
    }

    #[test]
    fn parses_debuginfod_urls() {
        let _guard = ENV_LOCK
            .get_or_init(|| StdMutex::new(()))
            .lock()
            .expect("environment lock");
        temp_env::with_var("KRABKA_PROFILES_DEBUGINFOD_URLS", None::<&str>, || {
            let cli = Cli::try_parse_from([
                "krabka-profiles",
                "--target",
                "querier",
                "--debuginfod-url",
                "http://one.example,http://two.example",
            ])
            .unwrap();

            assert!(
                cli.debuginfod_urls
                    == vec![
                        "http://one.example".to_string(),
                        "http://two.example".to_string()
                    ]
            );
        });
    }

    #[test]
    fn loads_tenant_limits_config_from_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("limits.json");
        std::fs::write(
            &path,
            r#"{
              "default": {
                "max_label_names_per_series": 10,
                "max_label_value": "100B",
                "session_id_buckets": 32
              },
              "tenants": {
                "tenant-a": {
                  "max_label_names_per_series": 2,
                  "max_label_value": "3B",
                  "session_id_buckets": 4
                }
              }
            }"#,
        )
        .unwrap();

        let config = load_tenant_limits_config(Some(&path)).unwrap();

        assert!(config.default.max_label_names_per_series == 10);
        assert!(config.for_tenant("tenant-a").max_label_value == bytes(3));
    }

    #[test]
    fn loads_profiles_limits_overrides_config_from_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overrides.yaml");
        std::fs::write(
            &path,
            r"
overrides:
  tenant-a:
    max_query_length_secs: 30
    max_flamegraph_nodes_max: 512
",
        )
        .unwrap();

        let overrides = load_profiles_limits_overrides_config(Some(&path)).unwrap();

        assert!(
            *overrides.for_tenant("tenant-a")
                == krabka_profiles::limits::Limits {
                    ingestion_rate: per_sec(10_000),
                    ingestion_burst_profiles: 10_000,
                    max_series: 0,
                    max_label_name: bytes(1024),
                    max_label_value: bytes(2048),
                    max_label_names_per_series: 40,
                    max_flamegraph_nodes_default: 2048,
                    max_flamegraph_nodes_max: 512,
                    max_query_length: secs(30),
                    max_session_id_cardinality: 0,
                }
        );
        // An unlisted tenant inherits the process default query-length cap.
        check!(
            overrides.for_tenant("tenant-b").max_query_length
                == krabka_profiles::limits::DEFAULT_MAX_QUERY_LENGTH
        );
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["krabka-profiles", "--target", "bogus"]).is_err());
    }
}

mod alloc;
mod build_object_store;
mod cli;
mod client_resource_policy;
mod configured_object_store;
mod debuginfod_config;
mod load_profiles_limits_overrides_config;
mod load_tenant_limits_config;
mod parse_client_dispatch_queue_capacity;
mod parse_client_frame_max;
mod parse_consumer_fetch_size;
mod parse_min_two_usize;
mod parse_non_empty_string;
mod parse_positive_time_or_legacy;
mod parse_positive_time_or_legacy_millis;
mod parse_positive_time_or_legacy_nanos;
mod parse_positive_usize;
mod parse_positive_whole_byte_size;
mod role_shutdown_token;
mod run;
mod spawn_profile_index_refresh;
mod spawn_wal_tail;
mod target;

#[cfg(all(unix, feature = "heap-profiling"))]
use alloc::ALLOC;

use build_object_store::build_object_store;
use cli::Cli;
use client_resource_policy::client_resource_policy;
use configured_object_store::ConfiguredObjectStore;
use debuginfod_config::debuginfod_config;
use load_profiles_limits_overrides_config::load_profiles_limits_overrides_config;
use load_tenant_limits_config::load_tenant_limits_config;
use parse_client_dispatch_queue_capacity::parse_client_dispatch_queue_capacity;
use parse_client_frame_max::parse_client_frame_max;
use parse_consumer_fetch_size::parse_consumer_fetch_size;
use parse_min_two_usize::parse_min_two_usize;
use parse_non_empty_string::parse_non_empty_string;
use parse_positive_time_or_legacy::parse_positive_time_or_legacy;
use parse_positive_time_or_legacy_millis::parse_positive_time_or_legacy_millis;
use parse_positive_time_or_legacy_nanos::parse_positive_time_or_legacy_nanos;
use parse_positive_usize::parse_positive_usize;
use parse_positive_whole_byte_size::parse_positive_whole_byte_size;
use role_shutdown_token::role_shutdown_token;
use run::run;
use spawn_profile_index_refresh::spawn_profile_index_refresh;
use spawn_wal_tail::spawn_wal_tail;
use target::Target;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let telemetry = krabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "krabka-profiles",
            env!("CARGO_PKG_VERSION"),
            "krabka-profiles",
        )?,
        "krabka_profiles=info,info",
        "info",
        "krabka-profiles",
    )?;
    let result = run(cli).await;
    telemetry.shutdown();
    result
}

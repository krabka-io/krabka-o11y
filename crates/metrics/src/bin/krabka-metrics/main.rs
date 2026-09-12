use std::{
    ffi::OsStr,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use clap::{
    Arg, Command, Parser, ValueEnum,
    builder::{EnumValueParser, PossibleValue, TypedValueParser},
    error::{Error, ErrorKind},
};
use krabka_blockstore::{
    BlockLevel, BlockTimestampUnit, BlockWriter, CompactionPolicy, DEFAULT_BLOCK_READ_MAX,
    DEFAULT_MAX_BLOCKS_PER_JOB, DEFAULT_MAX_LEVEL, DEFAULT_TARGET_ROWS_PER_BLOCK,
};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_core::{
    ClientFrameMax, ClientSecurity, ConnectionDispatchQueueCapacity,
    DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_client_producer::Producer;
use krabka_metrics::{
    DEFAULT_MAX_RATE_BUCKETS, Limits, MetricsCompactorConfig, ObjectStoreCompactionIndexSink,
    OverridesProvider,
    distributor::{
        DistributorState, HA_TRACKER_TOPIC, KafkaHaElectionSink, KafkaSink,
        router as distributor_router, run_ha_election_consumer_loop,
    },
    metrics::ServiceMetrics,
    run_compactor_consumer_loop,
};
use krabka_observability::{
    ConfigFileArgs, RoleReadiness, argv_with_config_file,
    audit::AuditArgs,
    readiness_router,
    server_security::{
        ServerListener, ServerSecurity, ServerSecurityArgs, install_crypto_provider, serve_router,
    },
    topic_contract::{METRICS_TOPICS, require_topics},
    wal_client_security::WalClientSecurityArgs,
};
use krabka_telemetry::OtlpConfig;
use krabka_units::{parse, prelude::*};
use object_store::ObjectStore;
use tokio::net::TcpListener;

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Mutex, OnceLock},
    };

    use assert2::{assert, check};
    use clap::{CommandFactory, Parser, ValueEnum as _};
    use krabka_broker::{Broker, BrokerConfig};
    use krabka_client_admin::{AdminClient, CreateTopicSpec};
    use krabka_observability::topic_contract::{METRICS_HA_TOPIC, METRICS_WAL_TOPIC};

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    /// One stage, one spelling. A `--target` value that drifted from the
    /// shared vocabulary would put a second name on a stage an operator
    /// already runs under another signal, which is exactly the hazard the
    /// vocabulary removes. The check goes through clap rather than through
    /// the enum's `Debug`, because the string clap accepts is the one a
    /// manifest carries.
    #[test]
    fn every_target_is_spelled_as_the_shared_vocabulary_spells_it() {
        for target in Target::value_variants() {
            let possible = target
                .to_possible_value()
                .expect("every target is a possible value");
            check!(possible.get_name() == target.kind().as_str(), "{target:?}");
            check!(
                Cli::try_parse_from([
                    "krabka-metrics",
                    "--target",
                    target.kind().as_str(),
                    "--bootstrap",
                    "broker:9092",
                ])
                .expect("the shared name parses")
                .target
                    == *target
            );
        }
    }

    /// A service that binds loopback inside a container is unreachable from
    /// outside the pod, and the symptom is a health check that fails with
    /// nothing in the logs. Prometheus and Mimir default their HTTP listener
    /// to every interface; so does this.
    #[test]
    fn default_listen_addresses_are_reachable_from_outside_the_container() {
        let cli = Cli::try_parse_from(["krabka-metrics", "--target", "distributor"]).unwrap();

        check!(cli.listen.ip().is_unspecified());
        check!(cli.listen.port() == 4041);
        check!(cli.admin_listen_addr.ip().is_unspecified());
    }

    /// The binary's own flags, out of a file. The generic precedence rules
    /// have their own suite; this one is here because a `Cli` that forgot to
    /// flatten `ConfigFileArgs` would pass every one of those and still
    /// ignore an operator's file.
    #[test]
    fn a_config_file_supplies_this_binary_s_flags() {
        // The same lock the environment tests take. `argv_with_config_file`
        // leaves out any flag clap has already sourced from the environment,
        // so a `KRABKA_METRICS_*` variable that another test sets and unsets
        // while this one runs removes `--target` from the argv and then
        // removes the environment it was deferring to. `Cli::parse_from` ends
        // the process on a usage error, so that race does not fail this test:
        // it kills the whole binary with exit 2 and reports nothing.
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("krabka.yaml");
        std::fs::write(&path, "target: block-builder\nlisten: 0.0.0.0:4444\n").unwrap();

        let argv = krabka_observability::argv_with_config_file::<Cli>(vec![
            "krabka-metrics".into(),
            "--config.file".into(),
            path.into_os_string(),
        ])
        .unwrap();
        let cli = Cli::parse_from(argv);

        check!(cli.target == Target::BlockBuilder);
        check!(cli.listen.port() == 4444);
    }

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults = Cli::try_parse_from(["krabka-metrics", "--target", "distributor"]).unwrap();
        assert!(defaults.client_dispatch_queue_capacity == 64);
        assert!(defaults.client_frame_max == krabka_units::mebibytes(100));

        let custom = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "distributor",
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
                "krabka-metrics",
                "--target",
                "distributor",
                "--client-dispatch-queue-capacity",
                "0",
            ],
            vec![
                "krabka-metrics",
                "--target",
                "distributor",
                "--client-frame-max",
                "101MiB",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_METRICS_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_METRICS_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("KRABKA_METRICS_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-metrics", "--target", "distributor"]).unwrap();
        assert!(from_env.client_dispatch_queue_capacity == 7);
        assert!(from_env.client_frame_max == krabka_units::kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "distributor",
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ])
        .unwrap();
        assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert!(from_cli.client_frame_max == krabka_units::kibibytes(64));
    }

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["krabka-metrics", "--target", "distributor"]).unwrap();

        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn distributor_policy_parses_defaults_overrides_and_boundaries() {
        let defaults = Cli::try_parse_from(["krabka-metrics", "--target", "distributor"]).unwrap();
        check!(
            defaults.ha_failover_timeout
                == krabka_metrics::distributor::DEFAULT_HA_FAILOVER_TIMEOUT
        );
        check!(defaults.ingest_rate_bucket_cap == DEFAULT_MAX_RATE_BUCKETS);
        check!(
            defaults.distributor_max_decompressed
                == krabka_metrics::distributor::DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED
        );
        check!(
            defaults
                .distributor_otel_promote_resource_attributes
                .is_empty()
        );

        let configured = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "distributor",
            "--ha-failover-timeout",
            "-1s",
            "--ingest-rate-bucket-cap",
            "7",
            "--distributor-max-decompressed",
            "64KiB",
            "--distributor.otel-promote-resource-attributes",
            "k8s.cluster.name,cloud.region",
        ])
        .unwrap();
        check!(configured.ha_failover_timeout == Time::from_millis(-1_000));
        check!(configured.ingest_rate_bucket_cap == 7);
        check!(configured.distributor_max_decompressed == kibibytes(64));
        check!(
            configured.distributor_otel_promote_resource_attributes
                == ["k8s.cluster.name", "cloud.region"]
        );

        for args in [
            ["--ingest-rate-bucket-cap", "0"],
            ["--distributor-max-decompressed", "0B"],
            ["--distributor-max-decompressed", "1.5B"],
        ] {
            let input = [
                "krabka-metrics",
                "--target",
                "distributor",
                args[0],
                args[1],
            ];
            assert!(Cli::try_parse_from(input).is_err());
        }
    }

    #[test]
    fn distributor_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_METRICS_DISTRIBUTOR_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::distributor_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_METRICS_HA_FAILOVER_TIMEOUT", "-1s")
                    .env("KRABKA_METRICS_INGEST_RATE_BUCKET_CAP", "7")
                    .env("KRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED", "64KiB")
                    .status()
                    .expect("child test");
            assert!(status.success());
            return;
        }

        let from_env = Cli::try_parse_from(["krabka-metrics", "--target", "distributor"]).unwrap();
        check!(from_env.ha_failover_timeout == Time::from_millis(-1_000));
        check!(from_env.ingest_rate_bucket_cap == 7);
        check!(from_env.distributor_max_decompressed == kibibytes(64));

        let from_cli = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "distributor",
            "--ha-failover-timeout",
            "5s",
            "--ingest-rate-bucket-cap",
            "9",
            "--distributor-max-decompressed",
            "128KiB",
        ])
        .unwrap();
        check!(from_cli.ha_failover_timeout == secs(5));
        check!(from_cli.ingest_rate_bucket_cap == 9);
        check!(from_cli.distributor_max_decompressed == kibibytes(128));
    }

    /// The distributor's per-tenant limits come from a runtime overrides
    /// file. Without the flag every tenant is pinned at the built-in
    /// defaults, with nothing able to move them. The flag and its environment
    /// variable are spelled as `krabka-metrics-service` spells them, so one
    /// file configures the write path and the read path together.
    #[test]
    fn parses_the_runtime_overrides_path() {
        let cli = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "distributor",
            "--runtime-overrides",
            "/etc/krabka/runtime.yaml",
        ])
        .unwrap();

        check!(cli.runtime_overrides == Some(PathBuf::from("/etc/krabka/runtime.yaml")));
    }

    /// The loader turns that path into the limits the distributor enforces.
    /// An absent path is not a failure, because no overrides file is the
    /// documented default; a named file that is not there is.
    #[test]
    fn loads_per_tenant_limits_from_the_named_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("runtime.yaml");
        std::fs::write(
            &path,
            "overrides:\n  tenant-tight:\n    max_series_per_request: 3\n",
        )
        .expect("write runtime overrides");

        let overrides = load_runtime_overrides(Some(path.as_path()))
            .expect("load runtime overrides")
            .expect("a named file yields a provider");
        check!(overrides.for_tenant("tenant-tight").max_series_per_request == 3);
        check!(
            overrides.for_tenant("tenant-other").max_series_per_request
                == krabka_metrics::Limits::default().max_series_per_request,
            "an unlisted tenant keeps the default"
        );

        check!(
            load_runtime_overrides(None)
                .expect("no path is not a failure")
                .is_none()
        );
        check!(load_runtime_overrides(Some(dir.path().join("absent.yaml").as_path())).is_err());
    }

    #[test]
    fn parses_distributor_ha_tracker_options() {
        let cli = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "distributor",
            "--ha-tracker-topic",
            "__tenant_a_ha",
            "--ha-tracker-group-id",
            "metrics-ha",
            "--ha-tracker-client-id",
            "metrics-ha-1",
            "--ha-tracker-poll-timeout",
            "250ms",
        ])
        .unwrap();

        check!(cli.ha_tracker_topic == "__tenant_a_ha");
        check!(cli.ha_tracker_group_id == "metrics-ha");
        check!(cli.ha_tracker_client_id == "metrics-ha-1");
        check!(cli.ha_tracker_poll_timeout == millis(250));
    }

    /// What an operator who runs the old command sees.
    ///
    /// `krabka-metrics --target=querier` used to bind the data port, log that
    /// it was listening, answer `/ready`, and serve Grafana's build-info probe
    /// -- enough for a datasource to read as healthy -- while 404-ing every
    /// query. The role is gone. Clap's own rejection would say only that the
    /// value is not one of two variants, which tells an operator holding a
    /// deployment that names `querier` nothing about where the querier went,
    /// so the message names the binary that serves it.
    #[test]
    fn a_retired_role_is_rejected_and_names_the_binary_that_serves_it() {
        for role in ["querier", "query-frontend", "ruler"] {
            let rendered = Cli::try_parse_from(["krabka-metrics", "--target", role])
                .expect_err("krabka-metrics has no read-path role")
                .to_string();

            check!(rendered.contains(role), "{role}: {rendered}");
            check!(
                rendered.contains("krabka-metrics-service"),
                "{role}: {rendered}"
            );
        }
    }

    /// The same refusal through the path a deployment actually takes.
    ///
    /// `//deploy` starts every role from a `--config.file`, and
    /// `argv_with_config_file` turns a `target:` key into `--target=<value>`
    /// before clap sees it. A manifest carrying a dead role has to fail there
    /// too, and with the same message.
    #[test]
    fn a_config_file_naming_a_retired_role_is_refused_the_same_way() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("krabka.yaml");
        std::fs::write(&path, "target: querier\nlisten: 0.0.0.0:9090\n").unwrap();

        let argv = krabka_observability::argv_with_config_file::<Cli>(vec![
            "krabka-metrics".into(),
            "--config.file".into(),
            path.into_os_string(),
        ])
        .unwrap();
        let rendered = Cli::try_parse_from(argv)
            .expect_err("a config file cannot name a role this binary does not have")
            .to_string();

        check!(rendered.contains("krabka-metrics-service"), "{rendered}");
    }

    /// Wrapping clap's enum parser rather than replacing it is what keeps
    /// `--help` able to answer "then which roles *does* it have". A bare
    /// `value_parser` function would drop the list entirely, and the only
    /// symptom would be a `--help` that names no role at all.
    #[test]
    fn the_help_offers_the_roles_this_binary_has_and_none_it_refuses() {
        let rendered = Cli::command().render_long_help().to_string();
        let (_, after) = rendered
            .split_once("Possible values:")
            .expect("`--target` renders the roles it accepts");
        let offered: String = after
            .lines()
            .skip(1)
            .take_while(|line| !line.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n");

        for role in Target::value_variants()
            .iter()
            .filter_map(ValueEnum::to_possible_value)
        {
            check!(offered.contains(role.get_name()), "{offered}");
        }
        for retired in ["querier", "query-frontend", "ruler"] {
            check!(!offered.contains(retired), "{retired}: {offered}");
        }
    }

    #[test]
    fn parses_compactor_runtime_options() {
        let cli = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "block-builder",
            "--bootstrap",
            "broker:9092",
            "--block-builder-group-id",
            "metrics-c",
            "--block-builder-poll-timeout",
            "250ms",
            "--block-builder-retention-sweep-interval",
            "30s",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::BlockBuilder));
        check!(cli.bootstrap == "broker:9092");
        check!(cli.block_builder_group_id == "metrics-c");
        check!(cli.block_builder_poll_timeout == millis(250));
        check!(cli.block_builder_retention_sweep_interval == secs(30));
    }

    #[test]
    fn runtime_options_read_unit_bearing_environment_values() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_vars(
            [
                ("KRABKA_METRICS_TARGET", Some("block-builder")),
                ("KRABKA_METRICS_BLOCK_BUILDER_POLL_TIMEOUT", Some("250ms")),
                ("KRABKA_METRICS_BLOCK_BUILDER_FLUSH_MAX_AGE", Some("2m")),
                (
                    "KRABKA_METRICS_BLOCK_BUILDER_RETENTION_SWEEP_INTERVAL",
                    Some("30s"),
                ),
            ],
            || {
                let cli = Cli::try_parse_from(["krabka-metrics"]).expect("parse environment");
                assert!(matches!(cli.target, Target::BlockBuilder));
                assert!(
                    (
                        cli.block_builder_poll_timeout,
                        cli.block_builder_flush_max_age,
                        cli.block_builder_retention_sweep_interval,
                    ) == (millis(250), minutes(2), secs(30))
                );
            },
        );
    }

    /// The compaction policy an operator configures, and the defaults it falls
    /// back to. The defaults are `krabka-blockstore`'s, not this binary's, so a
    /// drift between the two would give the metrics compactor a ladder no other
    /// signal has.
    #[test]
    fn parses_compactor_policy_options() {
        let defaults = Cli::try_parse_from(["krabka-metrics", "--target", "compactor"]).unwrap();
        check!(
            (
                defaults.compactor_max_blocks_per_job,
                defaults.compactor_target_rows,
                defaults.compactor_max_level,
                defaults.compactor_level_window,
            ) == (
                DEFAULT_MAX_BLOCKS_PER_JOB,
                DEFAULT_TARGET_ROWS_PER_BLOCK,
                DEFAULT_MAX_LEVEL.get(),
                krabka_blockstore::DEFAULT_LEVEL_WINDOW,
            )
        );
        check!(defaults.compactor_interval == minutes(5));

        let configured = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "compactor",
            "--compactor-max-blocks-per-job",
            "3",
            "--compactor-target-rows",
            "7",
            "--compactor-max-level",
            "2",
            "--compactor-level-window",
            "30m",
            "--compactor-interval",
            "45s",
        ])
        .unwrap();
        check!(
            (
                configured.compactor_max_blocks_per_job,
                configured.compactor_target_rows,
                configured.compactor_max_level,
                configured.compactor_level_window,
                configured.compactor_interval,
            ) == (3, 7, 2, minutes(30), secs(45))
        );

        // A metrics block counts epoch milliseconds, and a policy built with any
        // other unit puts every block into one bucket without saying so.
        let policy = compactor_policy_from_cli(&configured);
        check!(policy.timestamp_unit() == BlockTimestampUnit::Millis);
        check!(policy.max_level() == BlockLevel(2));
        check!(policy.window_ticks_for(BlockLevel(0)) == 30 * 60 * 1_000);

        // Every value the policy would silently clamp is refused here instead.
        for args in [
            ["--compactor-max-blocks-per-job", "1"],
            ["--compactor-target-rows", "0"],
            ["--compactor-max-level", "0"],
            ["--compactor-level-window", "0s"],
            ["--compactor-interval", "0s"],
        ] {
            check!(
                Cli::try_parse_from(["krabka-metrics", "--target", "compactor", args[0], args[1]])
                    .is_err(),
                "{args:?}"
            );
        }
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["krabka-metrics", "--target", "bogus"]).is_err());
    }

    /// The server, audit and write-ahead log security flags are the shared
    /// ones, flattened. With none set, the data port serves plain HTTP with no
    /// authentication, the audit layer is off, and the broker connections are
    /// plain text, as Grafana Mimir and the Kafka clients default to.
    #[test]
    fn parses_the_server_audit_and_wal_security_flags() {
        let defaults = Cli::try_parse_from(["krabka-metrics", "--target", "distributor"]).unwrap();
        check!(defaults.server_security.server_tls_cert_path.is_none());
        check!(defaults.server_security.auth_credentials_config.is_none());
        check!(
            defaults
                .server_security
                .internal_client_token_path
                .is_none()
        );
        check!(!defaults.audit.is_enabled());
        check!(defaults.wal_security == WalClientSecurityArgs::default());

        let configured = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "distributor",
            "--server-tls-cert-path",
            "/etc/krabka/tls.crt",
            "--server-tls-key-path",
            "/etc/krabka/tls.key",
            "--auth-credentials-config",
            "/etc/krabka/credentials.yaml",
            "--internal-client-token-path",
            "/etc/krabka/internal-token",
            "--audit-topic",
            "krabka-audit",
            "--audit-partition",
            "3",
            "--wal-security-protocol",
            "SASL_SSL",
            "--wal-tls-ca-path",
            "/etc/krabka/broker-ca.pem",
            "--wal-tls-server-name",
            "broker.internal",
            "--wal-sasl-mechanism",
            "SCRAM-SHA-512",
            "--wal-sasl-username",
            "krabka-metrics",
            "--wal-sasl-password-path",
            "/etc/krabka/wal-password",
        ])
        .unwrap();
        check!(
            configured.server_security.server_tls_cert_path
                == Some(PathBuf::from("/etc/krabka/tls.crt"))
        );
        check!(
            configured.server_security.server_tls_key_path
                == Some(PathBuf::from("/etc/krabka/tls.key"))
        );
        check!(
            configured.server_security.auth_credentials_config
                == Some(PathBuf::from("/etc/krabka/credentials.yaml"))
        );
        check!(
            configured.server_security.internal_client_token_path
                == Some(PathBuf::from("/etc/krabka/internal-token"))
        );
        check!(configured.audit.topic.as_deref() == Some("krabka-audit"));
        check!(configured.audit.partition == krabka_ids::PartitionIndex(3));
        check!(
            configured.wal_security
                == WalClientSecurityArgs {
                    wal_security_protocol:
                        krabka_observability::wal_client_security::WalSecurityProtocol::SaslSsl,
                    wal_tls_ca_path: Some(PathBuf::from("/etc/krabka/broker-ca.pem")),
                    wal_tls_server_name: Some("broker.internal".to_string()),
                    wal_sasl_mechanism: Some(
                        krabka_observability::wal_client_security::WalSaslMechanism::ScramSha512
                    ),
                    wal_sasl_username: Some("krabka-metrics".to_string()),
                    wal_sasl_password_path: Some(PathBuf::from("/etc/krabka/wal-password")),
                    ..WalClientSecurityArgs::default()
                }
        );
    }

    /// A security flag set that cannot work stops the start with a message
    /// that names the flag, before the role binds a port or reaches a broker.
    #[tokio::test]
    async fn a_security_flag_set_that_cannot_work_stops_the_start() {
        let cases = [
            (
                vec!["--server-tls-cert-path", "/etc/krabka/tls.crt"],
                "--server-tls-key-path",
            ),
            (vec!["--wal-security-protocol", "SSL"], "--wal-tls-ca-path"),
        ];

        for (flags, named) in cases {
            let mut argv = vec![
                "krabka-metrics",
                "--target",
                "distributor",
                "--admin-listen-addr",
                "127.0.0.1:0",
            ];
            argv.extend(flags);
            let cli = Cli::try_parse_from(argv).expect("the flags parse");

            let error = run(cli).await.expect_err("the start stops");

            check!(error.to_string().contains(named), "{error}");
        }
    }

    /// The contract has to stop a start, not merely describe one.
    ///
    /// The broker here holds a metrics WAL topic that meets the contract and
    /// an HA topic that does not: it carries no `cleanup.policy`, so the
    /// broker default of `delete` applies and every HA election is discarded
    /// at the retention window. A distributor or compactor started against it
    /// would produce and consume happily and lose the election map without a
    /// word. Both roles of this binary reach that broker, so both must refuse.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_topic_that_breaks_the_contract_stops_every_role_that_uses_it() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        create_topics(
            &bootstrap,
            &[
                (
                    METRICS_WAL_TOPIC,
                    BTreeMap::from([("retention.ms".to_string(), "900000".to_string())]),
                ),
                // The violation: a compacted state topic with no
                // `cleanup.policy` override on it.
                (METRICS_HA_TOPIC, BTreeMap::new()),
            ],
        )
        .await;

        for target in [Target::Distributor, Target::BlockBuilder] {
            let cli = cli_for(target, &bootstrap);
            let error = require_role_topics(&cli, None)
                .await
                .expect_err("a role that reaches this broker refuses to start");
            check!(error.to_string().contains(METRICS_HA_TOPIC), "{target:?}");
        }
    }

    /// A compactor reads and writes object storage and reaches no broker, so a
    /// broker it never opens must not be a condition of its starting. Before the
    /// check became role-aware, a compactor could not start without one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_compactor_starts_with_no_broker_to_answer_it() {
        // Bound and dropped, so the address is one nothing answers on.
        let unused = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let bootstrap = unused.local_addr().expect("local address").to_string();
        drop(unused);

        let cli = cli_for(Target::Compactor, &bootstrap);

        check!(require_role_topics(&cli, None).await.is_ok());
        check!(!cli.target.touches_the_wal());
    }

    /// An address nothing answers on is a broken contract too, not a silent
    /// pass. Both roles here produce or consume, so neither may start without
    /// having read the contract back off a broker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreachable_broker_stops_every_role() {
        // Bound and dropped, so the address is one nothing answers on.
        let unused = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let bootstrap = unused.local_addr().expect("local address").to_string();
        drop(unused);

        for target in [Target::Distributor, Target::BlockBuilder] {
            let cli = cli_for(target, &bootstrap);
            check!(require_role_topics(&cli, None).await.is_err(), "{target:?}");
        }
    }

    /// The refusal has to reach the process, not just the helper: `run` must
    /// return the contract error and leave its data port unbound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_role_does_not_start_and_does_not_listen_when_the_contract_is_broken() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        create_topics(&bootstrap, &[(METRICS_HA_TOPIC, BTreeMap::new())]).await;

        // A port nothing holds, so a bind by the role is the only thing that
        // could make it answer.
        let reserved = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let listen = reserved.local_addr().expect("local address");
        drop(reserved);

        let cli = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "distributor",
            "--bootstrap",
            &bootstrap,
            "--listen",
            &listen.to_string(),
            "--admin-listen-addr",
            "127.0.0.1:0",
        ])
        .expect("distributor cli");

        // Bounded: a role that does not refuse serves until it is stopped, so
        // without this the failure would be a hung test rather than a red one.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), run(cli))
            .await
            .expect("the role decides within 30s whether to start");

        let error = outcome.expect_err("the role refuses to start");
        check!(error.to_string().contains("topic contract violated"));
        assert!(tokio::net::TcpStream::connect(listen).await.is_err());
    }

    fn cli_for(target: Target, bootstrap: &str) -> Cli {
        let name = target.kind().as_str();
        Cli::try_parse_from(["krabka-metrics", "--target", name, "--bootstrap", bootstrap])
            .expect("cli")
    }

    async fn create_topics(bootstrap: &str, topics: &[(&str, BTreeMap<String, String>)]) {
        let mut admin = AdminClient::connect(&[bootstrap.to_string()])
            .await
            .expect("admin connect");
        let specs: Vec<CreateTopicSpec> = topics
            .iter()
            .map(|(name, configs)| CreateTopicSpec {
                name: (*name).to_string(),
                partitions: 1,
                replicas: 1,
                configs: configs.clone(),
            })
            .collect();
        admin
            .create_topics(&specs, krabka_units::secs(5))
            .await
            .expect("create topics");
    }
}

mod alloc;
mod build_object_store;
mod cli;
mod compactor_loop;
mod compactor_policy_from_cli;
mod ingest_rate_bucket_cap;
mod load_runtime_overrides;
mod parse_client_dispatch_queue_capacity;
mod parse_client_frame_max;
mod parse_compactor_max_blocks_per_job;
mod parse_compactor_max_level;
mod parse_compactor_target_rows;
mod parse_distributor_max_decompressed;
mod parse_ingest_rate_bucket_cap;
mod require_role_topics;
mod retired_role_message;
mod run;
mod run_block_builder;
mod run_compactor;
mod run_compactor_once;
mod run_distributor;
mod spawn_retention_sweeper;
mod target;
mod target_value_parser;

// `alloc` deliberately has no `use` line. `#[global_allocator]` registers
// the static by attribute, so naming it here imports something nothing
// reads -- which is a warning, not a link to the allocator.

use build_object_store::build_object_store;
use cli::Cli;
#[cfg_attr(test, mutants::skip)]
use compactor_loop::compactor_loop;
use compactor_policy_from_cli::compactor_policy_from_cli;
use ingest_rate_bucket_cap::IngestRateBucketCap;
use load_runtime_overrides::load_runtime_overrides;
use parse_client_dispatch_queue_capacity::parse_client_dispatch_queue_capacity;
use parse_client_frame_max::parse_client_frame_max;
use parse_compactor_max_blocks_per_job::parse_compactor_max_blocks_per_job;
use parse_compactor_max_level::parse_compactor_max_level;
use parse_compactor_target_rows::parse_compactor_target_rows;
use parse_distributor_max_decompressed::parse_distributor_max_decompressed;
use parse_ingest_rate_bucket_cap::parse_ingest_rate_bucket_cap;
use require_role_topics::require_role_topics;
use retired_role_message::retired_role_message;
#[cfg_attr(test, mutants::skip)]
use run::run;
use run_block_builder::run_block_builder;
#[cfg_attr(test, mutants::skip)]
use run_compactor::run_compactor;
use run_compactor_once::run_compactor_once;
use run_distributor::run_distributor;
#[cfg_attr(test, mutants::skip)]
use spawn_retention_sweeper::spawn_retention_sweeper;
use target::Target;
use target_value_parser::TargetValueParser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // First: rustls has two crypto providers compiled in, and the first TLS
    // client or server that asks for the process default panics without one.
    install_crypto_provider();
    let cli = Cli::parse_from(argv_with_config_file::<Cli>(std::env::args_os())?);
    let telemetry = krabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "krabka-metrics",
            env!("CARGO_PKG_VERSION"),
            "krabka-metrics",
        )?,
        "krabka_metrics=info,info",
        "info",
        "krabka-metrics",
    )?;
    let result = run(cli).await;
    telemetry.shutdown();
    result
}

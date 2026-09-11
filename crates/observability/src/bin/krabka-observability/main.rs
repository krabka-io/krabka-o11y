//! `krabka-observability` is a role-selectable Loki-compatible logs service.
//! It self-instruments with OTLP traces, JSON logs, and CPU and heap pprof.

use std::net::SocketAddr;

use clap::Parser;
use krabka_observability::{
    ClientResourcePolicy, ConfigFileArgs, RoleReadiness, ServiceConfig, argv_with_config_file,
    build_service_dependencies_with_client_resource_policy, init_telemetry,
    metrics::ServiceMetrics, readiness_router, serve_service,
    server_security::install_crypto_provider,
};
use krabka_units::{ByteSize, parse};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::{assert, check};
    use clap::Parser as _;
    use krabka_broker::{Broker, BrokerConfig};
    use krabka_client_admin::{AdminClient, CreateTopicSpec};
    use krabka_observability::{Role, topic_contract::LOGS_WAL_TOPIC};

    use super::{Cli, ServiceConfig, require_role_topics};

    #[test]
    fn client_resource_policy_parses_defaults_overrides_and_invalid_values() {
        let defaults =
            Cli::try_parse_from(["krabka-observability", "--target", "querier"]).expect("defaults");
        assert!(defaults.client_dispatch_queue_capacity == 64);
        assert!(defaults.client_frame_max == krabka_units::mebibytes(100));

        let custom = Cli::try_parse_from([
            "krabka-observability",
            "--target",
            "querier",
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
        ])
        .expect("custom policy");
        assert!(custom.client_dispatch_queue_capacity == 7);
        assert!(custom.client_frame_max == krabka_units::kibibytes(32));

        for option in [
            "--client-dispatch-queue-capacity=0",
            "--client-frame-max=101MiB",
            "--client-frame-max=1.5B",
        ] {
            Cli::try_parse_from(["krabka-observability", "--target", "querier", option])
                .expect_err(option);
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_TEST_OBSERVABILITY_CLIENT_POLICY_ENV_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let environment = Cli::try_parse_from(["krabka-observability", "--target", "querier"])
                .expect("environment policy");
            assert!(environment.client_dispatch_queue_capacity == 7);
            assert!(environment.client_frame_max == krabka_units::kibibytes(32));

            let cli = Cli::try_parse_from([
                "krabka-observability",
                "--target",
                "querier",
                "--client-dispatch-queue-capacity",
                "9",
                "--client-frame-max",
                "64KiB",
            ])
            .expect("CLI policy");
            assert!(cli.client_dispatch_queue_capacity == 9);
            assert!(cli.client_frame_max == krabka_units::kibibytes(64));
            return;
        }

        let status =
            std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("KRABKA_OBSERVABILITY_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                .env("KRABKA_OBSERVABILITY_CLIENT_FRAME_MAX", "32KiB")
                .status()
                .expect("run isolated environment parser test");
        assert!(status.success());
    }

    #[test]
    fn profiling_policy_flattens_cli_and_environment() {
        const CHILD: &str = "KRABKA_TEST_OBSERVABILITY_PROFILING_ENV_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let environment = Cli::try_parse_from(["krabka-observability", "--target", "querier"])
                .expect("environment profiling policy");
            assert!(environment.profiling.profiling_cpu_default_duration == krabka_units::secs(2));
            assert!(
                environment
                    .profiling
                    .profiling_cpu_sample_frequency
                    .frequency()
                    == krabka_units::per_sec(101)
            );

            let cli = Cli::try_parse_from([
                "krabka-observability",
                "--target",
                "querier",
                "--profiling-cpu-default-duration=3s",
                "--profiling-cpu-sample-frequency=103Hz",
            ])
            .expect("CLI profiling policy");
            assert!(cli.profiling.profiling_cpu_default_duration == krabka_units::secs(3));
            assert!(
                cli.profiling.profiling_cpu_sample_frequency.frequency()
                    == krabka_units::per_sec(103)
            );
            return;
        }

        let defaults = Cli::try_parse_from(["krabka-observability", "--target", "querier"])
            .expect("default profiling policy");
        assert!(defaults.profiling == krabka_telemetry::profiling::ProfilingConfig::default());

        let status =
            std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "tests::profiling_policy_flattens_cli_and_environment",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("KRABKA_PROFILING_CPU_DEFAULT_DURATION", "2s")
                .env("KRABKA_PROFILING_CPU_SAMPLE_FREQUENCY", "101Hz")
                .status()
                .expect("run isolated profiling environment parser test");
        assert!(status.success());
    }
    /// The contract has to stop a start, not merely describe one. A logs role
    /// pointed at a broker with no WAL topic on it would build a consumer that
    /// reads an empty stream and answer every query from local files alone,
    /// with nothing in the answer to say the WAL was never there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_missing_wal_topic_stops_every_role_that_names_a_broker() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();

        for target in [Role::Distributor, Role::BlockBuilder, Role::Querier] {
            let config = config_for(target, Some(bootstrap.clone()));
            let error = require_role_topics(&config, None)
                .await
                .expect_err("the role refuses to start");
            check!(error.to_string().contains(LOGS_WAL_TOPIC), "{target:?}");
        }

        create_logs_wal_topic(&bootstrap).await;

        for target in [Role::Distributor, Role::BlockBuilder, Role::Querier] {
            check!(
                require_role_topics(&config_for(target, Some(bootstrap.clone())), None)
                    .await
                    .is_ok(),
                "{target:?}"
            );
        }
    }

    /// Every logs role also runs with no WAL at all, reading and writing local
    /// files. With no broker named there is no topic to check, so the contract
    /// must not be the thing that stops it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_role_with_no_broker_configured_is_not_held_up_by_the_contract() {
        for target in [Role::Distributor, Role::BlockBuilder, Role::Querier] {
            check!(
                require_role_topics(&config_for(target, None), None)
                    .await
                    .is_ok(),
                "{target:?}"
            );
        }
    }

    fn config_for(target: Role, wal_bootstrap_server: Option<String>) -> ServiceConfig {
        ServiceConfig {
            target,
            wal_bootstrap_server,
            ..ServiceConfig::default()
        }
    }

    async fn create_logs_wal_topic(bootstrap: &str) {
        let mut admin = AdminClient::connect(&[bootstrap.to_string()])
            .await
            .expect("admin connect");
        admin
            .create_topics(
                &[CreateTopicSpec {
                    name: LOGS_WAL_TOPIC.to_string(),
                    partitions: 1,
                    replicas: 1,
                    configs: BTreeMap::from([("retention.ms".to_string(), "900000".to_string())]),
                }],
                krabka_units::secs(5),
            )
            .await
            .expect("create the logs WAL topic");
    }
}

mod alloc;
mod cli;
mod parse_dispatch_queue_capacity;
mod parse_frame_max;
mod require_role_topics;

// `alloc` deliberately has no `use` line. `#[global_allocator]` registers
// the static by attribute, so naming it here imports something nothing
// reads -- which is a warning, not a link to the allocator.

pub(crate) use cli::Cli;
pub(crate) use parse_dispatch_queue_capacity::parse_dispatch_queue_capacity;
pub(crate) use parse_frame_max::parse_frame_max;
pub(crate) use require_role_topics::require_role_topics;

#[tokio::main]
pub(crate) async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // First of all. The workspace compiles rustls with two crypto providers,
    // so the first TLS connection panics until one of them is installed. The
    // OTLP exporter, the WAL clients and the listener all open TLS.
    install_crypto_provider();
    let cli = Cli::parse_from(argv_with_config_file::<Cli>(std::env::args_os())?);
    let client_resource_policy = ClientResourcePolicy {
        dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(
            cli.client_dispatch_queue_capacity,
        )
        .expect("validated client dispatch queue capacity"),
        frame_max: krabka_client_core::ClientFrameMax::try_from(cli.client_frame_max)
            .expect("validated client frame maximum"),
    };
    // `init_telemetry`, not `krabka_telemetry::init`: without OTLP it installs
    // the same JSON stdout layer over a reloadable filter, which is what makes
    // `POST /log_level` move the level rather than report that it did.
    let (telemetry, _log_level) = init_telemetry(
        krabka_telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "krabka-logs",
            env!("CARGO_PKG_VERSION"),
            "krabka-logs",
        )?,
        "krabka_observability=info,info",
        "info",
        "krabka-logs",
    )?;
    // Loaded once, before any port opens and before any broker connection, so
    // a bad combination of security flags stops the start with its own error.
    let server_security = cli.service.server_security.load()?;
    let wal_security = cli.service.wal_client_security.load()?;
    let metrics = ServiceMetrics::new();
    // One readiness for the process: the role's own router reports it on the
    // data port, and the admin port echoes it, so a probe that cannot reach
    // the data port still gets the truth rather than "the listener is up".
    let readiness = RoleReadiness::new();
    // CPU/heap profiling admin server (Alloy pyroscope.scrape target) plus the
    // Prometheus RED-metrics exporter and `/ready` on the same :9404 admin port.
    krabka_telemetry::profiling::serve_admin_with_config(
        cli.admin_listen_addr,
        krabka_observability::metrics::metrics_router(metrics.registry.clone())
            .merge(readiness_router(readiness.clone())),
        cli.profiling.clone(),
    )
    .await?;

    let config = cli.service;
    // Before the WAL producer and consumer exist. A role that started against
    // a topic whose partition count is not the one the deployment provisioned
    // would read or write a re-mapped key space and report nothing.
    require_role_topics(&config, wal_security.clone()).await?;
    let dependencies = build_service_dependencies_with_client_resource_policy(
        &config,
        client_resource_policy,
        wal_security,
        metrics.wal_consumer.clone(),
    )
    .await?
    .with_metrics(metrics)
    .with_readiness(readiness)
    .with_server_security(server_security);
    serve_service(config, dependencies, None).await?;

    telemetry.shutdown();
    Ok(())
}

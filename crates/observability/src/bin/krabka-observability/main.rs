//! `krabka-observability` is a role-selectable Loki-compatible logs service.
//! It self-instruments with OTLP traces, JSON logs, and CPU and heap pprof.

use clap::Parser;
use krabka_observability::{
    ClientResourcePolicy, ServiceConfig, build_service_dependencies_with_client_resource_policy,
    metrics::ServiceMetrics, serve_service,
};
use krabka_units::{ByteSize, parse};

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::Cli;

    #[test]
    fn client_resource_policy_parses_defaults_overrides_and_invalid_values() {
        let defaults =
            Cli::try_parse_from(["krabka-observability", "--target", "querier"]).expect("defaults");
        assert_eq!(defaults.client_dispatch_queue_capacity, 64);
        assert_eq!(defaults.client_frame_max, krabka_units::mebibytes(100));

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
        assert_eq!(custom.client_dispatch_queue_capacity, 7);
        assert_eq!(custom.client_frame_max, krabka_units::kibibytes(32));

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
            assert_eq!(environment.client_dispatch_queue_capacity, 7);
            assert_eq!(environment.client_frame_max, krabka_units::kibibytes(32));

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
            assert_eq!(cli.client_dispatch_queue_capacity, 9);
            assert_eq!(cli.client_frame_max, krabka_units::kibibytes(64));
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
            assert_eq!(
                environment.profiling.profiling_cpu_default_duration,
                krabka_units::secs(2)
            );
            assert_eq!(
                environment
                    .profiling
                    .profiling_cpu_sample_frequency
                    .frequency(),
                krabka_units::per_sec(101)
            );

            let cli = Cli::try_parse_from([
                "krabka-observability",
                "--target",
                "querier",
                "--profiling-cpu-default-duration=3s",
                "--profiling-cpu-sample-frequency=103Hz",
            ])
            .expect("CLI profiling policy");
            assert_eq!(
                cli.profiling.profiling_cpu_default_duration,
                krabka_units::secs(3)
            );
            assert_eq!(
                cli.profiling.profiling_cpu_sample_frequency.frequency(),
                krabka_units::per_sec(103)
            );
            return;
        }

        let defaults = Cli::try_parse_from(["krabka-observability", "--target", "querier"])
            .expect("default profiling policy");
        assert_eq!(
            defaults.profiling,
            krabka_telemetry::profiling::ProfilingConfig::default()
        );

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
}

mod alloc;
mod cli;
mod parse_dispatch_queue_capacity;
mod parse_frame_max;

#[cfg(all(unix, feature = "heap-profiling"))]
pub(crate) use alloc::ALLOC;

pub(crate) use cli::Cli;
pub(crate) use parse_dispatch_queue_capacity::parse_dispatch_queue_capacity;
pub(crate) use parse_frame_max::parse_frame_max;

#[tokio::main]
pub(crate) async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client_resource_policy = ClientResourcePolicy {
        dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(
            cli.client_dispatch_queue_capacity,
        )
        .expect("validated client dispatch queue capacity"),
        frame_max: krabka_client_core::ClientFrameMax::try_from(cli.client_frame_max)
            .expect("validated client frame maximum"),
    };
    let telemetry = krabka_telemetry::init(
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
    let metrics = ServiceMetrics::new();
    // CPU/heap profiling admin server (Alloy pyroscope.scrape target) plus the
    // Prometheus RED-metrics exporter on the same :9404 admin port.
    krabka_telemetry::profiling::serve_admin_from_env_with_config(
        "0.0.0.0:9404",
        krabka_observability::metrics::metrics_router(metrics.registry.clone()),
        cli.profiling.clone(),
    )
    .await?;

    let config = cli.service;
    let dependencies =
        build_service_dependencies_with_client_resource_policy(&config, client_resource_policy)
            .await?
            .with_metrics(metrics);
    serve_service(config, dependencies, None).await?;

    telemetry.shutdown();
    Ok(())
}

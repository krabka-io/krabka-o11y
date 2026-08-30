use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use clap::{Parser, ValueEnum};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_client_producer::Producer;
use krabka_metrics::{
    DEFAULT_MAX_RATE_BUCKETS, MetricsCompactorConfig,
    distributor::{
        DistributorState, HA_TRACKER_TOPIC, KafkaHaElectionSink, KafkaSink,
        router as distributor_router, run_ha_election_consumer_loop,
    },
    metrics::ServiceMetrics,
    run_compactor_consumer_loop,
};
use krabka_telemetry::OtlpConfig;
use krabka_units::{parse, prelude::*};
use object_store::ObjectStore;
use serde_json::json;
use tokio::net::TcpListener;

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use assert2::{assert, check};
    use axum::{body::Body, http::Request};
    use clap::Parser;
    use tower::ServiceExt;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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
        ])
        .unwrap();
        check!(configured.ha_failover_timeout == Time::from_millis(-1_000));
        check!(configured.ingest_rate_bucket_cap == 7);
        check!(configured.distributor_max_decompressed == kibibytes(64));

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

    #[test]
    fn parses_query_frontend_target() {
        let cli = Cli::try_parse_from(["krabka-metrics", "--target", "query-frontend"]).unwrap();

        assert!(matches!(cli.target, Target::QueryFrontend));
    }

    #[tokio::test]
    async fn querier_router_serves_prometheus_build_info() {
        let response = querier_router()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/api/v1/status/buildinfo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
    }

    #[tokio::test]
    async fn querier_server_binds_to_listen_address() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let bound = serve_querier("127.0.0.1:0".parse().unwrap(), async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap();
        let _ = stop_tx.send(());

        assert!(bound.port() != 0);
    }

    #[tokio::test]
    async fn query_frontend_router_serves_prometheus_build_info() {
        let response = query_frontend_router()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/api/v1/status/buildinfo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
    }

    #[tokio::test]
    async fn query_frontend_server_binds_to_listen_address() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let bound = serve_query_frontend("127.0.0.1:0".parse().unwrap(), async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap();
        let _ = stop_tx.send(());

        assert!(bound.port() != 0);
    }

    #[tokio::test]
    async fn ruler_router_serves_prometheus_build_info() {
        let response = ruler_router()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/api/v1/status/buildinfo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
    }

    #[tokio::test]
    async fn ruler_server_binds_to_listen_address() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let bound = serve_ruler("127.0.0.1:0".parse().unwrap(), async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap();
        let _ = stop_tx.send(());

        assert!(bound.port() != 0);
    }

    #[test]
    fn parses_compactor_runtime_options() {
        let cli = Cli::try_parse_from([
            "krabka-metrics",
            "--target",
            "compactor",
            "--bootstrap",
            "broker:9092",
            "--compactor-group-id",
            "metrics-c",
            "--compactor-poll-timeout",
            "250ms",
            "--compactor-retention",
            "1h",
            "--compactor-retention-sweep-interval",
            "30s",
        ])
        .unwrap();

        assert!(matches!(cli.target, Target::Compactor));
        check!(cli.bootstrap == "broker:9092");
        check!(cli.compactor_group_id == "metrics-c");
        check!(cli.compactor_poll_timeout == millis(250));
        check!(cli.compactor_retention == hours(1));
        check!(cli.compactor_retention_sweep_interval == secs(30));
    }

    #[test]
    fn runtime_options_read_unit_bearing_environment_values() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_vars(
            [
                ("KRABKA_METRICS_TARGET", Some("compactor")),
                ("KRABKA_METRICS_COMPACTOR_POLL_TIMEOUT", Some("250ms")),
                ("KRABKA_METRICS_COMPACTOR_FLUSH_MAX_AGE", Some("2m")),
                ("KRABKA_METRICS_COMPACTOR_RETENTION", Some("1h")),
                (
                    "KRABKA_METRICS_COMPACTOR_RETENTION_SWEEP_INTERVAL",
                    Some("30s"),
                ),
            ],
            || {
                let cli = Cli::try_parse_from(["krabka-metrics"]).expect("parse environment");
                assert!(matches!(cli.target, Target::Compactor));
                assert!(
                    (
                        cli.compactor_poll_timeout,
                        cli.compactor_flush_max_age,
                        cli.compactor_retention,
                        cli.compactor_retention_sweep_interval,
                    ) == (millis(250), minutes(2), hours(1), secs(30))
                );
            },
        );
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["krabka-metrics", "--target", "bogus"]).is_err());
    }
}

mod alloc;
mod build_object_store;
mod cli;
mod ingest_rate_bucket_cap;
mod parse_client_dispatch_queue_capacity;
mod parse_client_frame_max;
mod parse_distributor_max_decompressed;
mod parse_ingest_rate_bucket_cap;
mod querier_build_info;
mod querier_router;
mod query_frontend_router;
mod role_build_info;
mod role_status_router;
mod ruler_router;
mod run_compactor;
mod run_distributor;
mod run_querier;
mod run_query_frontend;
mod run_ruler;
mod serve_querier;
mod serve_query_frontend;
mod serve_role_http;
mod serve_ruler;
mod spawn_retention_sweeper;
mod target;
mod unix_time_ms;

#[cfg(all(unix, feature = "heap-profiling"))]
use alloc::ALLOC;

use build_object_store::build_object_store;
use cli::Cli;
use ingest_rate_bucket_cap::IngestRateBucketCap;
use parse_client_dispatch_queue_capacity::parse_client_dispatch_queue_capacity;
use parse_client_frame_max::parse_client_frame_max;
use parse_distributor_max_decompressed::parse_distributor_max_decompressed;
use parse_ingest_rate_bucket_cap::parse_ingest_rate_bucket_cap;
use querier_build_info::querier_build_info;
use querier_router::querier_router;
use query_frontend_router::query_frontend_router;
use role_build_info::role_build_info;
use role_status_router::role_status_router;
use ruler_router::ruler_router;
#[cfg_attr(test, mutants::skip)]
use run_compactor::run_compactor;
use run_distributor::run_distributor;
use run_querier::run_querier;
use run_query_frontend::run_query_frontend;
use run_ruler::run_ruler;
#[cfg(test)]
use serve_querier::serve_querier;
#[cfg(test)]
use serve_query_frontend::serve_query_frontend;
#[cfg(test)]
use serve_role_http::serve_role_http;
#[cfg(test)]
use serve_ruler::serve_ruler;
#[cfg_attr(test, mutants::skip)]
use spawn_retention_sweeper::spawn_retention_sweeper;
use target::Target;
#[cfg_attr(test, mutants::skip)]
use unix_time_ms::unix_time_ms;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
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
    let result = async {
        let metrics = ServiceMetrics::new();
        let admin = krabka_telemetry::profiling::spawn_admin_with_config(
            cli.admin_listen_addr,
            krabka_metrics::metrics::metrics_router(metrics.registry.clone()),
            cli.profiling.clone(),
        )
        .await?;

        let role = async {
            match cli.target {
                Target::Distributor => run_distributor(cli, metrics).await?,
                Target::Compactor => run_compactor(cli, metrics).await?,
                Target::Querier => run_querier(cli).await?,
                Target::QueryFrontend => run_query_frontend(cli).await?,
                Target::Ruler => run_ruler(cli).await?,
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        };
        tokio::select! {
            result = role => result?,
            result = krabka_telemetry::profiling::await_admin_exit(admin) => result?,
        }
        Ok(())
    }
    .await;
    telemetry.shutdown();
    result
}

// Proving the async service futures `Send` traverses DataFusion's deep
// `sqlparser` AST type graph (reached through `SessionContext` held across
// awaits in the PromQL operator-path evaluation); the default limit is too low.
#![recursion_limit = "256"]

use std::{
    future::Future,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{Parser, ValueEnum};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_client_producer::Producer;
use krabka_metrics::{OverridesProvider, WAL_TOPIC};
use krabka_metrics_service::{
    KafkaRecordingRuleWalSink, KafkaRulerStateSink, PrometheusRulerStateSink, RULER_STATE_TOPIC,
    RulerAlertmanagerSink, RulerStateFanoutSink, WalHeadConsumerCommit, WalHeadConsumerPoll,
    install_bundled_rule_groups, run_ruler_evaluation_loop, run_ruler_state_consumer_loop,
    run_wal_head_consumer_loop, serve_prometheus_router_joinable,
};
use krabka_promql::{
    EngineOpts, PrometheusApiState, QueryFrontendOptions, RulerShard, WalHead, prometheus_router,
};
use krabka_telemetry::OtlpConfig;
use krabka_units::{parse, prelude::*};
use object_store::ObjectStore;

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use clap::Parser;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn parses_querier_target() {
        let cli = Cli::try_parse_from(["krabka-metrics-service", "--target", "querier"]).unwrap();

        assert2::assert!(matches!(cli.target, Target::Querier));
    }

    #[test]
    fn parses_query_frontend_target_and_options() {
        let cli = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "query-frontend",
            "--query-frontend-split",
            "30s",
            "--query-frontend-shards",
            "4",
            "--query-frontend-cache-prefix",
            "tenant-a-query-cache",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::QueryFrontend));
        assert2::assert!(cli.query_frontend_split == secs(30));
        assert2::assert!(cli.query_frontend_shards == 4);
        assert2::assert!(cli.query_frontend_cache_prefix.as_str() == "tenant-a-query-cache");
    }

    #[test]
    fn parses_ruler_target_and_options() {
        let cli = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "ruler",
            "--ruler-tenant",
            "tenant-a",
            "--ruler-eval-interval",
            "15s",
            "--ruler-shard-index",
            "2",
            "--ruler-shard-total",
            "4",
            "--ruler-alertmanager-url",
            "http://alertmanager.example/api/v2/alerts",
            "--ruler-state-topic",
            "__tenant_a_ruler_state",
            "--ruler-bundled-rules",
            "/etc/krabka/rules/krabka-clock.yaml",
        ])
        .unwrap();

        assert2::assert!(matches!(cli.target, Target::Ruler));
        assert2::assert!(cli.ruler_tenant.as_str() == "tenant-a");
        assert2::assert!(cli.ruler_eval_interval == secs(15));
        assert2::assert!(cli.ruler_shard_index == 2);
        assert2::assert!(cli.ruler_shard_total == 4);
        assert2::assert!(
            cli.ruler_alertmanager_url.as_deref()
                == Some("http://alertmanager.example/api/v2/alerts")
        );
        assert2::assert!(cli.ruler_state_topic.as_str() == "__tenant_a_ruler_state");
        assert2::assert!(
            cli.ruler_bundled_rules.as_deref()
                == Some(Path::new("/etc/krabka/rules/krabka-clock.yaml"))
        );
    }

    #[test]
    fn ruler_bundled_rules_are_absent_by_default() {
        let cli = Cli::try_parse_from(["krabka-metrics-service", "--target", "ruler"]).unwrap();

        assert2::assert!(cli.ruler_bundled_rules.is_none());
    }

    #[test]
    fn parses_listen_address() {
        let cli = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "querier",
            "--listen",
            "127.0.0.1:0",
        ])
        .unwrap();

        assert2::assert!(cli.listen.port() == 0);
    }

    #[test]
    fn parses_blockstore_querier_options() {
        let cli = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "querier",
            "--object-store-url",
            "file:///tmp/krabka-metrics",
            "--manifest-prefix",
            "metrics/tenant-a",
        ])
        .unwrap();

        assert2::assert!(&cli.object_store_url == &"file:///tmp/krabka-metrics".to_string());
        assert2::assert!(&cli.manifest_prefix == &"metrics/tenant-a".to_string());
    }

    #[test]
    fn cold_store_policy_parses_defaults_overrides_and_boundaries() {
        let defaults =
            Cli::try_parse_from(["krabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(defaults.cold_cache_ttl == krabka_metrics_service::DEFAULT_COLD_CACHE_TTL);
        assert2::assert!(
            defaults.unbounded_compatibility_lookback
                == krabka_metrics_service::DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK
        );

        let configured = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "querier",
            "--cold-cache-ttl",
            "5s",
            "--unbounded-compatibility-lookback",
            "10m",
        ])
        .unwrap();
        assert2::assert!(configured.cold_cache_ttl == secs(5));
        assert2::assert!(configured.unbounded_compatibility_lookback == minutes(10));

        for args in [
            ["--cold-cache-ttl", "0s"],
            ["--cold-cache-ttl", "-1s"],
            ["--unbounded-compatibility-lookback", "0s"],
            ["--unbounded-compatibility-lookback", "-1s"],
        ] {
            assert2::assert!(
                Cli::try_parse_from([
                    "krabka-metrics-service",
                    "--target",
                    "querier",
                    args[0],
                    args[1],
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn cold_store_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_METRICS_SERVICE_COLD_STORE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::cold_store_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_METRICS_COLD_CACHE_TTL", "5s")
                    .env("KRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK", "10m")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["krabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(from_env.cold_cache_ttl == secs(5));
        assert2::assert!(from_env.unbounded_compatibility_lookback == minutes(10));

        let from_cli = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "querier",
            "--cold-cache-ttl",
            "7s",
            "--unbounded-compatibility-lookback",
            "20m",
        ])
        .unwrap();
        assert2::assert!(from_cli.cold_cache_ttl == secs(7));
        assert2::assert!(from_cli.unbounded_compatibility_lookback == minutes(20));
    }

    #[test]
    fn query_policy_parses_defaults_overrides_and_boundaries() {
        let defaults =
            Cli::try_parse_from(["krabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(defaults.query_lookback_delta == minutes(5));
        assert2::assert!(defaults.query_eval_interval == minutes(1));
        assert2::assert!(defaults.query_max_samples == 50_000_000);
        assert2::assert!(defaults.remote_read_max_body == mebibytes(64));

        let configured = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "querier",
            "--query-lookback-delta=7m",
            "--query-eval-interval=11s",
            "--query-max-samples=13",
            "--remote-read-max-body=17MiB",
        ])
        .unwrap();
        assert2::assert!(query_engine_opts(&configured).lookback_delta == minutes(7));
        assert2::assert!(query_engine_opts(&configured).eval_interval == secs(11));
        assert2::assert!(query_engine_opts(&configured).max_samples == 13);
        assert2::assert!(configured.remote_read_max_body == mebibytes(17));

        for flag in [
            "--query-lookback-delta=0s",
            "--query-eval-interval=0s",
            "--query-max-samples=0",
            "--remote-read-max-body=0B",
            "--remote-read-max-body=1.5B",
        ] {
            assert2::assert!(
                Cli::try_parse_from(["krabka-metrics-service", "--target", "querier", flag,])
                    .is_err(),
                "accepted {flag}"
            );
        }
    }

    #[test]
    fn query_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "KRABKA_METRICS_QUERY_POLICY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::query_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_METRICS_QUERY_LOOKBACK_DELTA", "7m")
                    .env("KRABKA_METRICS_QUERY_EVAL_INTERVAL", "11s")
                    .env("KRABKA_METRICS_QUERY_MAX_SAMPLES", "13")
                    .env("KRABKA_METRICS_REMOTE_READ_MAX_BODY", "17MiB")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["krabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(query_engine_opts(&from_env).lookback_delta == minutes(7));
        assert2::assert!(query_engine_opts(&from_env).eval_interval == secs(11));
        assert2::assert!(query_engine_opts(&from_env).max_samples == 13);
        assert2::assert!(from_env.remote_read_max_body == mebibytes(17));

        let from_cli = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "querier",
            "--query-lookback-delta=19m",
            "--query-eval-interval=23s",
            "--query-max-samples=29",
            "--remote-read-max-body=31MiB",
        ])
        .unwrap();
        assert2::assert!(query_engine_opts(&from_cli).lookback_delta == minutes(19));
        assert2::assert!(query_engine_opts(&from_cli).eval_interval == secs(23));
        assert2::assert!(query_engine_opts(&from_cli).max_samples == 29);
        assert2::assert!(from_cli.remote_read_max_body == mebibytes(31));
    }

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults =
            Cli::try_parse_from(["krabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(defaults.client_dispatch_queue_capacity == 64);
        assert2::assert!(defaults.client_frame_max == mebibytes(100));

        let custom = Cli::try_parse_from([
            "krabka-metrics-service",
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
                "krabka-metrics-service",
                "--target",
                "querier",
                "--client-dispatch-queue-capacity",
                "0",
            ],
            vec![
                "krabka-metrics-service",
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
        const CHILD: &str = "KRABKA_METRICS_SERVICE_CLIENT_RESOURCE_POLICY_CHILD";

        if std::env::var_os(CHILD).is_none() {
            let status =
                std::process::Command::new(std::env::current_exe().expect("test executable"))
                    .args([
                        "--exact",
                        "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    ])
                    .env(CHILD, "1")
                    .env("KRABKA_METRICS_SERVICE_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                    .env("KRABKA_METRICS_SERVICE_CLIENT_FRAME_MAX", "32KiB")
                    .status()
                    .expect("child test");
            assert2::assert!(status.success());
            return;
        }

        let from_env =
            Cli::try_parse_from(["krabka-metrics-service", "--target", "querier"]).unwrap();
        assert2::assert!(from_env.client_dispatch_queue_capacity == 7);
        assert2::assert!(from_env.client_frame_max == kibibytes(32));

        let from_cli = Cli::try_parse_from([
            "krabka-metrics-service",
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
    fn parses_runtime_overrides_path() {
        let cli = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "query-frontend",
            "--runtime-overrides",
            "/etc/krabka/runtime.yaml",
        ])
        .unwrap();

        assert2::assert!(
            cli.runtime_overrides == Some(std::path::PathBuf::from("/etc/krabka/runtime.yaml"))
        );
    }

    #[test]
    fn parses_querier_wal_head_options() {
        let cli = Cli::try_parse_from([
            "krabka-metrics-service",
            "--target",
            "querier",
            "--wal-bootstrap",
            "127.0.0.1:9092",
            "--wal-group-id",
            "metrics-querier",
            "--wal-client-id",
            "querier-a",
            "--wal-topic",
            "__krabka_metrics_wal",
            "--wal-head-retention",
            "10m",
        ])
        .unwrap();

        assert2::assert!(cli.wal_bootstrap.as_deref() == Some("127.0.0.1:9092"));
        assert2::assert!(cli.wal_group_id.as_str() == "metrics-querier");
        assert2::assert!(cli.wal_client_id.as_str() == "querier-a");
        assert2::assert!(cli.wal_topic.as_str() == "__krabka_metrics_wal");
        assert2::assert!(cli.wal_head_retention == minutes(10));
    }

    #[test]
    fn runtime_options_read_unit_bearing_environment_values() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_vars(
            [
                ("KRABKA_METRICS_SERVICE_TARGET", Some("querier")),
                ("KRABKA_METRICS_WAL_POLL_TIMEOUT", Some("250ms")),
                ("KRABKA_METRICS_QUERIER_WAL_HEAD_RETENTION", Some("10m")),
            ],
            || {
                let cli =
                    Cli::try_parse_from(["krabka-metrics-service"]).expect("parse environment");
                assert2::assert!(matches!(cli.target, Target::Querier));
                assert2::assert!(
                    (cli.wal_poll_timeout, cli.wal_head_retention) == (millis(250), minutes(10))
                );
            },
        );
    }

    #[test]
    fn rejects_unknown_target() {
        assert2::assert!(
            Cli::try_parse_from(["krabka-metrics-service", "--target", "bogus"]).is_err()
        );
    }

    #[tokio::test]
    async fn shutdown_signalled_resolves_after_trigger() {
        let shutdown = Shutdown::new();
        let signalled = shutdown.signalled();
        // Trigger from another task; the server's graceful-shutdown hook (this
        // `signalled` future) must then resolve so the join can drain.
        let trigger = shutdown.clone();
        tokio::spawn(async move {
            trigger.trigger();
        });
        signalled.await;
    }

    #[tokio::test]
    async fn shutdown_signalled_resolves_immediately_when_already_triggered() {
        // A background task that triggers shutdown before the server begins its
        // drain (e.g. a critical consumer dies during startup) must not leave the
        // graceful-shutdown future hanging: `watch` retains the latest value, so a
        // receiver cloned after the trigger observes it on first poll.
        let shutdown = Shutdown::new();
        shutdown.trigger();
        shutdown.signalled().await;
    }

    #[tokio::test]
    async fn shutdown_stop_predicate_observes_trigger() {
        // The background loops' stop predicate borrows a cloned receiver; flipping
        // the shared shutdown must make that borrow read `true`.
        let shutdown = Shutdown::new();
        let stop = shutdown.rx.clone();
        assert2::assert!(!*stop.borrow());
        shutdown.trigger();
        assert2::assert!(*stop.borrow());
    }

    #[tokio::test]
    async fn wal_head_consumer_startup_runs_in_background() {
        let shutdown = Shutdown::new();
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();

        let task = spawn_wal_head_consumer_task(
            || async move {
                let _ = rx.await;
                Ok(PendingWalHeadConsumer)
            },
            krabka_promql::WalHead::new(),
            "__krabka_metrics_wal".to_string(),
            millis(1),
            shutdown.clone(),
        );

        let signalled = tokio::time::timeout(millis(25).to_std(), shutdown.signalled()).await;
        task.abort();

        assert2::assert!(signalled.is_err());
    }

    struct PendingWalHeadConsumer;

    #[async_trait::async_trait]
    impl krabka_metrics_service::WalHeadConsumerPoll for PendingWalHeadConsumer {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<
            Vec<krabka_client_consumer::ConsumerRecord>,
            krabka_metrics_service::WalHeadConsumerError,
        > {
            std::future::pending().await
        }
    }

    #[async_trait::async_trait]
    impl krabka_metrics_service::WalHeadConsumerCommit for PendingWalHeadConsumer {
        async fn commit_sync(
            &mut self,
        ) -> Result<(), krabka_metrics_service::WalHeadConsumerError> {
            Ok(())
        }
    }
}

mod alloc;
mod cli;
mod load_runtime_overrides;
mod parse_client_dispatch_queue_capacity;
mod parse_client_frame_max;
mod parse_positive_usize;
mod parse_remote_read_max_body;
mod query_engine_opts;
mod run_querier;
mod run_query_frontend;
mod run_ruler;
mod shutdown;
mod spawn_shutdown_signal_listener;
mod spawn_wal_head_consumer_task;
mod target;

#[cfg(all(unix, feature = "heap-profiling"))]
use alloc::ALLOC;

use cli::Cli;
use load_runtime_overrides::load_runtime_overrides;
use parse_client_dispatch_queue_capacity::parse_client_dispatch_queue_capacity;
use parse_client_frame_max::parse_client_frame_max;
use parse_positive_usize::parse_positive_usize;
use parse_remote_read_max_body::parse_remote_read_max_body;
use query_engine_opts::query_engine_opts;
use run_querier::run_querier;
use run_query_frontend::run_query_frontend;
use run_ruler::run_ruler;
use shutdown::Shutdown;
use spawn_shutdown_signal_listener::spawn_shutdown_signal_listener;
use spawn_wal_head_consumer_task::spawn_wal_head_consumer_task;
use target::Target;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let telemetry = krabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "metrics-service",
            env!("CARGO_PKG_VERSION"),
            "krabka-metrics-service",
        )?,
        "krabka_metrics_service=info,info",
        "info",
        "krabka-metrics-service",
    )?;
    let result = async {
        let metrics = krabka_promql::metrics::ServiceMetrics::new();
        let admin = krabka_telemetry::profiling::spawn_admin_from_env_with_config(
            "0.0.0.0:9404",
            krabka_promql::metrics::metrics_router(metrics.registry.clone()),
            cli.profiling.clone(),
        )
        .await?;

        let role = async {
            match cli.target {
                Target::Querier => run_querier(cli, metrics).await?,
                Target::QueryFrontend => run_query_frontend(cli, metrics).await?,
                Target::Ruler => run_ruler(cli, metrics).await?,
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

use clap::Parser as _;
use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};

use crate::tests::prelude::{
    Arc, BlockIndex, BufferedLogHotTail, ClientResourcePolicy, CompactionFrontierSource,
    DeferredWalConsumerConnect, InMemoryWalSink, IngestLimitError, LabelIndex, LogIngestLimiter,
    LogQueryAuthorizer, Principal, QuerierState, QueryAuthorizationError, ServiceConfig,
    ServiceDependencies, ServiceMetrics, SharedCompactionFrontier, StreamScanOptions, TenantId,
    WalLogRecord, admin_connection_options, async_trait, build_service_dependencies, check, days,
    hours, mebibytes, millis, minutes, next_compactor_object_store_backoff, secs,
    validate_compactor_policy, validate_distributor_policy, with_querier_dependencies,
};


#[test]
pub(crate) fn compactor_policy_uses_defaults_and_cli_overrides() {
    let defaults = ServiceConfig::default();
    check!(defaults.compactor_wal_poll_timeout == millis(500));
    check!(defaults.compactor_accumulation_window == secs(2));
    check!(defaults.compactor_accumulation_poll_timeout == millis(250));
    check!(defaults.compactor_max_records_per_batch.get() == 4096);
    check!(defaults.compactor_idle_interval == millis(10));
    check!(defaults.compactor_object_store_initial_backoff == millis(10));
    check!(defaults.compactor_object_store_max_backoff == millis(500));

    let configured = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target=block-builder",
        "--compactor-wal-poll-timeout=600ms",
        "--compactor-accumulation-window=3s",
        "--compactor-accumulation-poll-timeout=300ms",
        "--compactor-max-records-per-batch=5000",
        "--compactor-idle-interval=20ms",
        "--compactor-object-store-initial-backoff=20ms",
        "--compactor-object-store-max-backoff=600ms",
    ])
    .expect("valid compactor policy");
    check!(configured.compactor_wal_poll_timeout == millis(600));
    check!(configured.compactor_accumulation_window == secs(3));
    check!(configured.compactor_accumulation_poll_timeout == millis(300));
    check!(configured.compactor_max_records_per_batch.get() == 5000);
    check!(configured.compactor_idle_interval == millis(20));
    check!(configured.compactor_object_store_initial_backoff == millis(20));
    check!(configured.compactor_object_store_max_backoff == millis(600));
}

#[test]
pub(crate) fn querier_policy_rejects_zero() {
    for argument in [
        "--querier-frontier-refresh-interval=0s",
        "--querier-dynamic-index-cache-ttl=0s",
        "--querier-shard-index-cache-ttl=0s",
        "--querier-shard-fetch-concurrency=0",
        "--querier-cold-block-fetch-concurrency=0",
        "--querier-hot-tail-bucket-width=0s",
        "--querier-hot-tail-interval=0s",
        "--querier-dependency-reconnect-interval=0s",
    ] {
        check!(
            ServiceConfig::try_parse_from(["krabka-observability", "--target=querier", argument,])
                .is_err(),
            "accepted {argument}"
        );
    }
}

#[test]
pub(crate) fn querier_policy_uses_defaults_and_cli_overrides() {
    let defaults = ServiceConfig::default();
    check!(defaults.querier_frontier_refresh_interval == secs(5));
    check!(defaults.querier_dynamic_index_cache_ttl == secs(5));
    check!(defaults.querier_shard_index_cache_ttl == minutes(5));
    check!(defaults.querier_shard_fetch_concurrency.get() == 32);
    check!(defaults.querier_cold_block_fetch_concurrency.get() == 8);
    check!(defaults.querier_query_frontend_split_interval == hours(1));
    check!(defaults.querier_query_frontend_max_parallelism.get() == 14);
    check!(defaults.querier_query_frontend_max_retries == 5);
    check!(defaults.querier_query_frontend_cache_ttl == days(7));
    check!(defaults.querier_query_frontend_max_cache_freshness == minutes(10));
    check!(defaults.querier_query_frontend_target_bytes_per_shard == mebibytes(600));
    check!(defaults.querier_hot_tail_bucket_width == minutes(1));
    check!(defaults.querier_hot_tail_interval == millis(50));
    check!(defaults.querier_dependency_reconnect_interval == millis(500));

    let configured = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target=querier",
        "--querier-frontier-refresh-interval=6s",
        "--querier-dynamic-index-cache-ttl=7s",
        "--querier-shard-index-cache-ttl=6m",
        "--querier-shard-fetch-concurrency=33",
        "--querier-cold-block-fetch-concurrency=9",
        "--querier-query-frontend-split-interval=2h",
        "--querier-query-frontend-max-parallelism=15",
        "--querier-query-frontend-max-retries=6",
        "--querier-query-frontend-cache-ttl=6d",
        "--querier-query-frontend-max-cache-freshness=11m",
        "--querier-query-frontend-target-bytes-per-shard=700MiB",
        "--querier-hot-tail-bucket-width=2m",
        "--querier-hot-tail-interval=60ms",
        "--querier-dependency-reconnect-interval=600ms",
    ])
    .expect("valid querier policy");
    check!(configured.querier_frontier_refresh_interval == secs(6));
    check!(configured.querier_dynamic_index_cache_ttl == secs(7));
    check!(configured.querier_shard_index_cache_ttl == minutes(6));
    check!(configured.querier_shard_fetch_concurrency.get() == 33);
    check!(configured.querier_cold_block_fetch_concurrency.get() == 9);
    check!(configured.querier_query_frontend_split_interval == hours(2));
    check!(configured.querier_query_frontend_max_parallelism.get() == 15);
    check!(configured.querier_query_frontend_max_retries == 6);
    check!(configured.querier_query_frontend_cache_ttl == days(6));
    check!(configured.querier_query_frontend_max_cache_freshness == minutes(11));
    check!(configured.querier_query_frontend_target_bytes_per_shard == mebibytes(700));
    check!(configured.querier_hot_tail_bucket_width == minutes(2));
    check!(configured.querier_hot_tail_interval == millis(60));
    check!(configured.querier_dependency_reconnect_interval == millis(600));

    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_runtime_policy(&configured);
    check!(state.dynamic_index_cache.cache_ttl == secs(7));
    check!(state.dynamic_index_cache.shard_cache_ttl == minutes(6));
    check!(state.dynamic_index_cache.shard_fetch_concurrency.get() == 33);
    check!(state.cold_block_fetch_concurrency.get() == 9);
    check!(state.query_frontend_options.max_parallelism.get() == 15);
    check!(state.query_frontend_options.max_retries == 6);
    check!(state.query_frontend_options.max_cache_freshness == minutes(11).to_std());
    check!(state.query_frontend_split_ns == hours(2).nanos_i64());
    check!(state.query_frontend_target_bytes == mebibytes(700).bytes_u64());
    check!(
        StreamScanOptions::exhaustive()
            .with_block_fetch_concurrency(state.cold_block_fetch_concurrency)
            .block_fetch_concurrency()
            == 9
    );
}

struct EqualBoundaryCase {
    window: &'static str,
    timeout: &'static str,
}

#[test]
pub(crate) fn compactor_policy_rejects_zero_and_invalid_bounds() {
    for argument in [
        "--compactor-wal-poll-timeout=0s",
        "--compactor-accumulation-window=0s",
        "--compactor-accumulation-poll-timeout=0s",
        "--compactor-max-records-per-batch=0",
        "--compactor-idle-interval=0s",
        "--compactor-object-store-initial-backoff=0s",
        "--compactor-object-store-max-backoff=0s",
    ] {
        check!(
            ServiceConfig::try_parse_from([
                "krabka-observability",
                "--target=block-builder",
                argument,
            ])
            .is_err(),
            "accepted {argument}"
        );
    }

    let poll_above_window = ServiceConfig::parse_from([
        "krabka-observability",
        "--target=block-builder",
        "--compactor-accumulation-window=1s",
        "--compactor-accumulation-poll-timeout=2s",
    ]);
    check!(validate_compactor_policy(&poll_above_window).is_err());

    let initial_above_max = ServiceConfig::parse_from([
        "krabka-observability",
        "--target=block-builder",
        "--compactor-object-store-initial-backoff=2s",
        "--compactor-object-store-max-backoff=1s",
    ]);
    check!(validate_compactor_policy(&initial_above_max).is_err());

    // And the same pair of boundaries here.
    for case in [
        EqualBoundaryCase {
            window: "--compactor-accumulation-window=1s",
            timeout: "--compactor-accumulation-poll-timeout=1s",
        },
        EqualBoundaryCase {
            window: "--compactor-object-store-initial-backoff=1s",
            timeout: "--compactor-object-store-max-backoff=1s",
        },
    ] {
        let at_the_limit = ServiceConfig::parse_from([
            "krabka-observability",
            "--target=block-builder",
            case.window,
            case.timeout,
        ]);
        check!(
            validate_compactor_policy(&at_the_limit).is_ok(),
            "{} with {}",
            case.window,
            case.timeout
        );
    }
}

#[tokio::test]
pub(crate) async fn distributor_dependency_startup_rejects_invalid_policy_before_connecting() {
    let config = ServiceConfig::parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--wal-bootstrap-server=127.0.0.1:1",
        "--wal-connect-startup-deadline=1s",
        "--wal-connect-attempt-timeout=2s",
    ]);

    let Err(error) = build_service_dependencies(
        &config,
        crate::wal_consumer_metrics::WalConsumerMetrics::unregistered(),
    )
    .await
    else {
        panic!("invalid policy must fail before broker connection");
    };
    check!(
        error
            .to_string()
            .contains("must not exceed startup deadline")
    );
}

struct DistributorEqualBoundaryCase {
    deadline: &'static str,
    timeout: &'static str,
}

#[test]
pub(crate) fn distributor_policy_rejects_zero_and_invalid_bounds() {
    for argument in [
        "--reject-old-samples-max-age=0s",
        "--creation-grace-period=0s",
        "--ingest-quota-burst-window=0s",
        "--broker-access-cache-ttl=0s",
        "--broker-access-max-staleness=0s",
        "--broker-access-tenant-capacity=0",
        "--wal-connect-startup-deadline=0s",
        "--wal-connect-attempt-timeout=0s",
        "--wal-connect-initial-backoff=0s",
        "--wal-connect-max-backoff=0s",
    ] {
        check!(
            ServiceConfig::try_parse_from([
                "krabka-observability",
                "--target",
                "distributor",
                argument,
            ])
            .is_err(),
            "accepted {argument}"
        );
    }

    let attempt_above_deadline = ServiceConfig::parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--wal-connect-startup-deadline=1s",
        "--wal-connect-attempt-timeout=2s",
    ]);
    check!(validate_distributor_policy(&attempt_above_deadline).is_err());

    let initial_above_max = ServiceConfig::parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--wal-connect-initial-backoff=2s",
        "--wal-connect-max-backoff=1s",
    ]);
    check!(validate_distributor_policy(&initial_above_max).is_err());

    // Equal is not "exceeds". Both cases above are rejections, so the
    // comparisons could have refused a timeout that merely *matches* its
    // deadline and nothing would have noticed.
    for case in [
        DistributorEqualBoundaryCase {
            deadline: "--wal-connect-startup-deadline=1s",
            timeout: "--wal-connect-attempt-timeout=1s",
        },
        DistributorEqualBoundaryCase {
            deadline: "--wal-connect-initial-backoff=1s",
            timeout: "--wal-connect-max-backoff=1s",
        },
    ] {
        let at_the_limit = ServiceConfig::parse_from([
            "krabka-observability",
            "--target",
            "distributor",
            case.deadline,
            case.timeout,
        ]);
        check!(
            validate_distributor_policy(&at_the_limit).is_ok(),
            "{} with {}",
            case.deadline,
            case.timeout
        );
    }
}

struct BackoffCase {
    current: krabka_units::Time,
    expected: krabka_units::Time,
}

#[test]
pub(crate) fn retry_backoff_doubles_and_caps() {
    for case in [
        BackoffCase {
            current: millis(10),
            expected: millis(20),
        },
        BackoffCase {
            current: millis(300),
            expected: millis(500),
        },
        BackoffCase {
            current: millis(500),
            expected: millis(500),
        },
    ] {
        check!(next_compactor_object_store_backoff(case.current, millis(500)) == case.expected);
    }
    check!(next_compactor_object_store_backoff(millis(300), millis(400)) == millis(400));
}

#[test]
pub(crate) fn service_dependencies_builder_methods_preserve_existing_fields() {
    #[derive(Clone)]
    struct TestLimiter;
    #[async_trait]
    impl LogIngestLimiter for TestLimiter {
        async fn check(
            &self,
            _principal: &Principal,
            _tenant: &TenantId,
            _records: &[WalLogRecord],
        ) -> Result<(), IngestLimitError> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestAuthorizer;
    #[async_trait]
    impl LogQueryAuthorizer for TestAuthorizer {
        async fn check(
            &self,
            _principal: &Principal,
            _tenant: &TenantId,
        ) -> Result<(), QueryAuthorizationError> {
            Ok(())
        }
    }

    let metrics = ServiceMetrics::new();
    let frontier = SharedCompactionFrontier::default();
    let client_resource_policy = ClientResourcePolicy {
        dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(7)
            .unwrap(),
        frame_max: krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32))
            .unwrap(),
    };
    let deps = ServiceDependencies::default()
        .with_metrics(metrics.clone())
        .with_wal_sink(InMemoryWalSink::default())
        .with_ingest_limiter(TestLimiter)
        .with_query_authorizer(TestAuthorizer)
        .with_hot_tail_shared_frontier(BufferedLogHotTail::default(), frontier.clone())
        .with_deferred_wal_consumer_connect(DeferredWalConsumerConnect {
            bootstrap: "broker:9092".to_string(),
            group_id: "group".to_string(),
            topic: "topic".to_string(),
            client_resource_policy,
            security: None,
            metrics: crate::wal_consumer_metrics::WalConsumerMetrics::unregistered(),
        });

    check!(deps.metrics.is_some());
    check!(deps.wal_sink.is_some());
    check!(deps.ingest_limiter.is_some());
    check!(deps.query_authorizer.is_some());
    check!(deps.hot_tail.is_some());
    check!(deps.deferred_wal_consumer_connect.is_some());
    check!(Arc::ptr_eq(
        &deps.metrics.as_ref().unwrap().registry,
        &metrics.registry
    ));
    match deps.hot_tail.as_ref().unwrap().frontier.clone() {
        CompactionFrontierSource::Shared(actual) => {
            check!(actual.snapshot() == frontier.snapshot());
        }
        CompactionFrontierSource::Snapshot(_) => panic!("expected shared frontier"),
    }
    let deferred = deps.deferred_wal_consumer_connect.as_ref().unwrap();
    check!(
        (
            deferred.bootstrap.as_str(),
            deferred.group_id.as_str(),
            deferred.topic.as_str(),
            deferred.client_resource_policy,
            deferred.security.is_none(),
        ) == (
            "broker:9092",
            "group",
            "topic",
            client_resource_policy,
            true
        )
    );
    let options = admin_connection_options(client_resource_policy, None);
    check!(
        (
            options.dispatch_queue_capacity,
            options.frame_max,
            options.security.is_none()
        ) == (
            client_resource_policy.dispatch_queue_capacity,
            client_resource_policy.frame_max,
            true
        )
    );
}

mod wal_client_security_flags_reach_every_wal_connection {
use krabka_client_core::{ClientSecurity, SaslCredentials};
use krabka_security::ListenerProtocol;

use super::*;
use crate::tests::prelude::WalDependenciesRequest;

#[derive(Clone, Debug, Eq, PartialEq)]
struct PlainCredentials {
    protocol: ListenerProtocol,
    username: String,
    password: String,
}

struct SecurityCase<'a> {
    config: &'a ServiceConfig,
    expected: Option<PlainCredentials>,
}

// The SASL user name and password that a policy carries, or `None`.
fn plain_credentials_for_test(security: Option<&ClientSecurity>) -> Option<PlainCredentials> {
    let security = security?;
    match security.sasl.as_ref()? {
        SaslCredentials::Plain { username, password } => Some(PlainCredentials {
            protocol: security.protocol,
            username: username.clone(),
            password: password.clone(),
        }),
        _ => None,
    }
}

// The WAL client security flags parse through the `ServiceConfig`, and the
// policy they give reaches the admin connection of the query authorizer and
// the ingest limiter, and both deferred connects of the querier. With no
// flag set, every one of them connects in plain text.
#[test]
pub(crate) fn wal_client_security_flags_reach_every_wal_connection() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let password_path = dir.path().join("wal-password");
    std::fs::write(&password_path, "wal-secret\n").expect("the password file writes");
    let password_flag = format!("--wal-sasl-password-path={}", password_path.display());
    let base = [
        "krabka-observability",
        "--target=querier",
        "--wal-bootstrap-server=broker:9092",
    ];
    let secured = ServiceConfig::try_parse_from(base.iter().copied().chain([
        "--wal-security-protocol=SASL_PLAINTEXT",
        "--wal-sasl-mechanism=PLAIN",
        "--wal-sasl-username=krabka-logs",
        password_flag.as_str(),
    ]))
    .expect("the secured flags parse");
    let plain = ServiceConfig::try_parse_from(base).expect("the plain flags parse");
    let policy = ClientResourcePolicy::default();
    let expected = Some(PlainCredentials {
        protocol: ListenerProtocol::SaslPlaintext,
        username: "krabka-logs".to_string(),
        password: "wal-secret".to_string(),
    });

    for case in [
        SecurityCase {
            config: &secured,
            expected,
        },
        SecurityCase {
            config: &plain,
            expected: None,
        },
    ] {
        let config = case.config;
        let expected = case.expected;
        let security = config.wal_client_security.load().expect("the flags load");
        let options = admin_connection_options(policy, security.as_ref());
        let dependencies = with_querier_dependencies(WalDependenciesRequest {
            dependencies: ServiceDependencies::default(),
            config,
            group_id: "group".to_string(),
            client_resource_policy: policy,
            security: security.as_ref(),
            metrics: crate::wal_consumer_metrics::WalConsumerMetrics::unregistered(),
        })
        .expect("the querier dependencies build");
        let authorizer = dependencies
            .deferred_query_authorizer_connect
            .as_ref()
            .expect("a deferred authorizer connect");
        let consumer = dependencies
            .deferred_wal_consumer_connect
            .as_ref()
            .expect("a deferred consumer connect");

        check!(
            [
                plain_credentials_for_test(security.as_ref()),
                plain_credentials_for_test(options.security.as_deref()),
                plain_credentials_for_test(authorizer.security.as_ref()),
                plain_credentials_for_test(consumer.security.as_ref()),
            ] == [
                expected.clone(),
                expected.clone(),
                expected.clone(),
                expected.clone()
            ],
            "{:?}",
            config.wal_client_security
        );
    }
}
}

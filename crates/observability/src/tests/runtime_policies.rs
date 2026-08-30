use clap::Parser as _;

use super::prelude::{
    Arc, BlockIndex, BufferedLogHotTail, ClientResourcePolicy, CompactionFrontierSource,
    InMemoryWalSink, IngestLimitError, LabelIndex, LogIngestLimiter, LogQueryAuthorizer,
    QuerierState, QueryAuthorizationError, ServiceConfig, ServiceDependencies, ServiceMetrics,
    SharedCompactionFrontier, StreamScanOptions, WalLogRecord, admin_connection_options,
    async_trait, build_service_dependencies, check, millis, minutes,
    next_compactor_object_store_backoff, secs, validate_compactor_policy,
    validate_distributor_policy,
};
#[test]
pub(crate) fn distributor_policy_rejects_zero_and_invalid_bounds() {
    for argument in [
        "--reject-old-samples-max-age=0s",
        "--creation-grace-period=0s",
        "--ingest-quota-burst-window=0s",
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
    for (deadline, timeout) in [
        (
            "--wal-connect-startup-deadline=1s",
            "--wal-connect-attempt-timeout=1s",
        ),
        (
            "--wal-connect-initial-backoff=1s",
            "--wal-connect-max-backoff=1s",
        ),
    ] {
        let at_the_limit = ServiceConfig::parse_from([
            "krabka-observability",
            "--target",
            "distributor",
            deadline,
            timeout,
        ]);
        check!(
            validate_distributor_policy(&at_the_limit).is_ok(),
            "{deadline} with {timeout}"
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

    let Err(error) = build_service_dependencies(&config).await else {
        panic!("invalid policy must fail before broker connection");
    };
    check!(
        error
            .to_string()
            .contains("must not exceed startup deadline")
    );
}

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
        "--target=compactor",
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
            ServiceConfig::try_parse_from(
                ["krabka-observability", "--target=compactor", argument,]
            )
            .is_err(),
            "accepted {argument}"
        );
    }

    let poll_above_window = ServiceConfig::parse_from([
        "krabka-observability",
        "--target=compactor",
        "--compactor-accumulation-window=1s",
        "--compactor-accumulation-poll-timeout=2s",
    ]);
    check!(validate_compactor_policy(&poll_above_window).is_err());

    let initial_above_max = ServiceConfig::parse_from([
        "krabka-observability",
        "--target=compactor",
        "--compactor-object-store-initial-backoff=2s",
        "--compactor-object-store-max-backoff=1s",
    ]);
    check!(validate_compactor_policy(&initial_above_max).is_err());

    // And the same pair of boundaries here.
    for (window, timeout) in [
        (
            "--compactor-accumulation-window=1s",
            "--compactor-accumulation-poll-timeout=1s",
        ),
        (
            "--compactor-object-store-initial-backoff=1s",
            "--compactor-object-store-max-backoff=1s",
        ),
    ] {
        let at_the_limit = ServiceConfig::parse_from([
            "krabka-observability",
            "--target=compactor",
            window,
            timeout,
        ]);
        check!(
            validate_compactor_policy(&at_the_limit).is_ok(),
            "{window} with {timeout}"
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
    check!(configured.querier_hot_tail_bucket_width == minutes(2));
    check!(configured.querier_hot_tail_interval == millis(60));
    check!(configured.querier_dependency_reconnect_interval == millis(600));

    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_runtime_policy(&configured);
    check!(state.dynamic_index_cache.cache_ttl == secs(7));
    check!(state.dynamic_index_cache.shard_cache_ttl == minutes(6));
    check!(state.dynamic_index_cache.shard_fetch_concurrency.get() == 33);
    check!(state.cold_block_fetch_concurrency.get() == 9);
    check!(
        StreamScanOptions::exhaustive()
            .with_block_fetch_concurrency(state.cold_block_fetch_concurrency)
            .block_fetch_concurrency()
            == 9
    );
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
pub(crate) fn service_dependencies_builder_methods_preserve_existing_fields() {
    #[derive(Clone)]
    struct TestLimiter;
    #[async_trait]
    impl LogIngestLimiter for TestLimiter {
        async fn check(
            &self,
            _tenant: &str,
            _records: &[WalLogRecord],
        ) -> Result<(), IngestLimitError> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestAuthorizer;
    #[async_trait]
    impl LogQueryAuthorizer for TestAuthorizer {
        async fn check(&self, _tenant: &str) -> Result<(), QueryAuthorizationError> {
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
        .with_deferred_wal_consumer_connect(
            "broker:9092".to_string(),
            "group".to_string(),
            "topic".to_string(),
            client_resource_policy,
        );

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
            assert_eq!(actual.snapshot(), frontier.snapshot());
        }
        CompactionFrontierSource::Snapshot(_) => panic!("expected shared frontier"),
    }
    let deferred = deps.deferred_wal_consumer_connect.as_ref().unwrap();
    assert_eq!(deferred.bootstrap, "broker:9092");
    assert_eq!(deferred.group_id, "group");
    assert_eq!(deferred.topic, "topic");
    assert_eq!(deferred.client_resource_policy, client_resource_policy);
    let options = admin_connection_options(client_resource_policy);
    assert_eq!(
        options.dispatch_queue_capacity,
        client_resource_policy.dispatch_queue_capacity
    );
    assert_eq!(options.frame_max, client_resource_policy.frame_max);
}

#[test]
pub(crate) fn retry_backoff_doubles_and_caps() {
    for (current, want) in [
        (millis(10), millis(20)),
        (millis(300), millis(500)),
        (millis(500), millis(500)),
    ] {
        check!(next_compactor_object_store_backoff(current, millis(500)) == want);
    }
    check!(next_compactor_object_store_backoff(millis(300), millis(400)) == millis(400));
}

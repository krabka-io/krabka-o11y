use super::*;

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

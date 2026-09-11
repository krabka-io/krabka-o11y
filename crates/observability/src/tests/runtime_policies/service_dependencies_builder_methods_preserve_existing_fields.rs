use super::*;

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

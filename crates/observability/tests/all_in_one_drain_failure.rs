//! A successful stop must mean the final WAL drain succeeded.

use async_trait::async_trait;
use krabka_client_consumer::ConsumerError;
use krabka_observability::{
    CancellationToken, KafkaWalRecord, LogWalConsumer, LogWalSink, QuerierIndexSource, Role,
    ServiceConfig, ServiceDependencies, ServiceRuntimeError, WalConsumerError, WalLogRecord,
    WalPosition, WalSinkError, serve_all_service_listener,
};
use krabka_units::{Time, millis};
use object_store::memory::InMemory;
use tokio::net::TcpListener;

struct EmptyWal;

#[async_trait]
impl LogWalSink for EmptyWal {
    async fn append(&self, _record: WalLogRecord) -> Result<(), WalSinkError> {
        panic!("this shutdown fixture accepts no pushes")
    }
}

#[async_trait]
impl LogWalConsumer for EmptyWal {
    async fn poll(&mut self, _timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        tokio::task::yield_now().await;
        Ok(Vec::new())
    }

    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        panic!("an empty WAL has no offsets to commit")
    }
}

struct UndrainedWal {
    fail_after_drain_check: bool,
    draining: bool,
}

#[async_trait]
impl LogWalConsumer for UndrainedWal {
    async fn poll(&mut self, timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        if self.draining && self.fail_after_drain_check {
            return Err(ConsumerError::CoordinatorUnavailable.into());
        }
        EmptyWal.poll(timeout).await
    }

    async fn is_drained(&mut self) -> bool {
        self.draining = true;
        false
    }

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError> {
        EmptyWal.commit_compacted(position).await
    }
}

async fn stop_with(consumer: impl LogWalConsumer) -> Result<(), ServiceRuntimeError> {
    let dir = tempfile::tempdir().unwrap();
    let config = ServiceConfig {
        target: Role::All,
        tenant: Some("fixture".to_string()),
        query_start_ns: Some(0),
        query_end_ns: Some(1),
        data_root: dir.path().to_path_buf(),
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        index_prefix: Some("logs".to_string()),
        all_drain_stage_timeout: millis(25),
        ..ServiceConfig::default()
    };
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let store = InMemory::new();
    Box::pin(serve_all_service_listener(
        listener,
        config,
        ServiceDependencies::default()
            .with_wal_sink(EmptyWal)
            .with_wal_consumer(consumer),
        Some(&store),
        shutdown,
    ))
    .await
}

#[tokio::test]
async fn an_empty_finite_wal_stops_successfully() {
    stop_with(EmptyWal).await.unwrap();
}

#[tokio::test]
async fn a_failed_final_drain_is_returned_to_the_caller() {
    let error = stop_with(UndrainedWal {
        fail_after_drain_check: true,
        draining: false,
    })
    .await
    .unwrap_err();
    assert!(
        matches!(error, ServiceRuntimeError::Compactor(_)),
        "{error}"
    );
    assert!(error.to_string().contains("coordinator"), "{error}");
}

#[tokio::test]
async fn a_wal_that_never_catches_up_returns_a_drain_timeout() {
    let error = stop_with(UndrainedWal {
        fail_after_drain_check: false,
        draining: false,
    })
    .await
    .unwrap_err();
    let ServiceRuntimeError::Io(error) = error else {
        panic!("expected a drain timeout, got {error}");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert!(error.to_string().contains("drain the WAL"));
}

//! The profiles block-builder must ride out an object store that fails and
//! recovers, and must stop at once on one that never will.
//!
//! A flush writes the block, then the profile index snapshot, then commits the
//! WAL offset. Before this, any object-store error at any of those steps left
//! `run_with_config`, left `main`, and ended the role. Nothing was lost --
//! offsets sit behind the write -- but the role came back cold and re-read the
//! same window, so a fault outlasting a restart became an unbounded retry at
//! process granularity. This suite boots a real broker and asserts the two
//! halves of the replacement: a transient failure is ridden out and the offset
//! is committed exactly once, and a permanent one is reported on the first
//! attempt with the offset left where it was.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert2::{assert, check};
use async_trait::async_trait;
use krabka_blockstore::ObjectStoreRetryPolicy;
use object_store::{ObjectStore, PutPayload, path::Path};

use self::{
    block_builder_support::{
        ConsumedBuilder, OneRecordBroker, RestartConsumer, indexed_block_count,
    },
    hooked_store::{HookedStore, StoreHooks},
};

mod block_builder_support;
#[path = "../../blockstore/tests/support/hooked_store.rs"]
mod hooked_store;

/// A 5xx, a timeout or a reset connection -- what the `object_store` clients
/// report once their own retry budget is spent.
fn transient_failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "FlakyIndexStore",
        source: "injected 503".into(),
    }
}

/// A credential the store refuses. Waiting does not change the answer.
fn permanent_failure() -> object_store::Error {
    object_store::Error::PermissionDenied {
        path: "index".to_string(),
        source: "injected 403".into(),
    }
}

/// The hooks of an in-memory store whose first `failures` writes *under the
/// index prefix* fail with `error`.
///
/// Only the index writes are made to fail, because the index store is the one
/// the block-builder wraps with [`BlockBuilderConfig::object_store_retry`].
/// The block write is retried by the `BlockWriter` under its own policy and
/// has its own coverage in `krabka-blockstore`; leaving it alone here keeps
/// this test's schedule entirely injected, so nothing sleeps.
struct FlakyIndexHooks {
    remaining_failures: AtomicUsize,
    index_put_attempts: AtomicUsize,
    error: fn() -> object_store::Error,
}

type FlakyIndexStore = HookedStore<FlakyIndexHooks>;

impl FlakyIndexHooks {
    fn store(failures: usize, error: fn() -> object_store::Error) -> FlakyIndexStore {
        HookedStore::new(Self {
            remaining_failures: AtomicUsize::new(failures),
            index_put_attempts: AtomicUsize::new(0),
            error,
        })
    }

    fn index_put_attempts(&self) -> usize {
        self.index_put_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StoreHooks for FlakyIndexHooks {
    const NAME: &'static str = "FlakyIndexStore";

    async fn before_put(&self, location: &Path, _payload: &PutPayload) -> object_store::Result<()> {
        if location.as_ref().starts_with("index/") {
            self.index_put_attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .remaining_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    (left > 0).then(|| left - 1)
                })
                .is_ok()
            {
                return Err((self.error)());
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_rides_out_a_transient_object_store_and_commits_once() {
    let broker = TestBroker::start("krabka-profiles-retry-transient").await;
    let flaky = Arc::new(FlakyIndexHooks::store(2, transient_failure));
    let store: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;

    let (drained, index_key) = broker.drain_with(&store, &flaky).await;

    assert!(drained.is_ok());
    // The snapshot really was retried, not merely written once.
    check!(flaky.index_put_attempts() > 2);
    // The buffered record became a durable block ...
    assert!(indexed_block_count(&store, &index_key).await > 0);
    // ... and the offset behind it was committed exactly once, so a restart in
    // the same group replays nothing.
    check!(broker.replayed_records().await == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_refused_by_the_store_fails_at_once_and_leaves_the_offset() {
    let broker = TestBroker::start("krabka-profiles-retry-permanent").await;
    let flaky = Arc::new(FlakyIndexHooks::store(usize::MAX, permanent_failure));
    let store: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;

    let (drained, _index_key) = broker.drain_with(&store, &flaky).await;

    // Reported rather than retried: a 403 will read the same in four seconds.
    assert!(drained.is_err());
    check!(flaky.index_put_attempts() == 1);
    // And the offset stays behind the data the flush failed to make durable,
    // so a restart re-reads the record instead of losing it.
    check!(broker.replayed_records().await == 1);
}

/// A broker with the profiles WAL topic and one buffered record in it, and
/// the consumer group its block-builders join.
struct TestBroker {
    broker: OneRecordBroker,
    group_id: String,
}

impl TestBroker {
    async fn start(group_id: &str) -> Self {
        Self {
            broker: OneRecordBroker::start().await,
            group_id: group_id.to_string(),
        }
    }

    /// Runs a block-builder against `store` until the record is buffered, then
    /// cancels it so the drain flush happens, and returns what the drain
    /// returned along with the index key it used.
    async fn drain_with(
        &self,
        store: &Arc<dyn ObjectStore>,
        flaky: &Arc<FlakyIndexStore>,
    ) -> (Result<(), krabka_profiles::ProfilesError>, String) {
        let (mut config, metrics) = self.broker.drain_only_config(store, &self.group_id);
        let index_key = config.index_key.clone();
        // The injected schedule: the same number of attempts the default
        // allows, with none of its waiting. Raising the default cannot make
        // this suite slower.
        config.object_store_retry = ObjectStoreRetryPolicy::immediate(4);

        let builder = ConsumedBuilder::start(config, &metrics).await;
        check!(
            flaky.index_put_attempts() == 0,
            "no ordinary flush may fire before the drain"
        );

        (builder.drain().await, index_key)
    }

    /// What a restart in the block-builder's own consumer group would be
    /// handed. An uncommitted offset replays the record; a committed one does
    /// not.
    async fn replayed_records(&self) -> usize {
        self.broker
            .replayed_records(RestartConsumer {
                group_id: &self.group_id,
                client_id: "profiles-block-builder-retry-restart",
            })
            .await
    }
}

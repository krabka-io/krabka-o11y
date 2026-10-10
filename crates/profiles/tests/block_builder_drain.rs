//! The profiles block-builder must flush and commit what it holds when it is
//! asked to stop.
//!
//! The block-builder buffers WAL records and writes a block only once the
//! buffer is full or old enough. A stop that returns from the middle of that
//! window throws the buffer away and leaves the offset behind it uncommitted,
//! so the next start replays the window -- which is what every rolling restart
//! is. This suite boots a real broker, puts a record in the WAL that no
//! ordinary flush rule will reach, cancels the builder, and asserts on the
//! block and the committed offset the drain is supposed to leave behind.

use std::sync::Arc;

use assert2::{assert, check};
use object_store::{ObjectStore, memory::InMemory};

use self::block_builder_support::{
    ConsumedBuilder, OneRecordBroker, RestartConsumer, indexed_block_count,
};

mod block_builder_support;

const GROUP_ID: &str = "krabka-profiles-block-builder-drain";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_the_block_builder_flushes_and_commits_what_it_buffered() {
    let broker = OneRecordBroker::start().await;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (config, metrics) = broker.drain_only_config(&store, GROUP_ID);
    let index_key = config.index_key.clone();

    // Start the drain only after the WAL record has actually been consumed.
    let builder = ConsumedBuilder::start(config, &metrics).await;
    check!(
        indexed_block_count(&store, &index_key).await == 0,
        "no ordinary flush may fire before the drain"
    );

    let drained = builder.drain().await;
    assert!(drained.is_ok());

    // The buffered record became a durable block ...
    assert!(indexed_block_count(&store, &index_key).await > 0);
    // ... and the offset behind it was committed, so a restart in the same
    // group replays nothing.
    assert!(
        broker
            .replayed_records(RestartConsumer {
                group_id: GROUP_ID,
                client_id: "profiles-block-builder-drain-restart",
            })
            .await
            == 0
    );
}

use assert2::assert;
use krabka_units::{convert::TimeExt as _, millis};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::compactor::runtime::spawn_compaction_frontier_refresher;

/// Shutdown is not a failure.
///
/// A supervisor that treats every exit of this task as fatal would fail the
/// process on every clean stop, so the handle has to distinguish the two: a
/// cancelled refresher returns, and its handle joins with `Ok`.
#[tokio::test]
async fn a_cancelled_frontier_refresher_joins_cleanly_through_its_handle() {
    let token = CancellationToken::new();
    let handle = spawn_compaction_frontier_refresher(
        Arc::new(object_store::memory::InMemory::new()),
        ObjectPath::default(),
        SharedCompactionFrontier::default(),
        BufferedLogHotTail::default(),
        token.clone(),
        millis(50),
    );

    assert!(!handle.is_finished());
    token.cancel();

    let joined = tokio::time::timeout(millis(2_000).to_std(), handle)
        .await
        .expect("a cancelled refresher returns, so its handle resolves");
    assert!(let Ok(()) = joined);
}

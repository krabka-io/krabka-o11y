use assert2::assert;
use krabka_units::millis;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::compactor::runtime::spawn_compaction_frontier_refresher;

/// An object store that dies when the frontier is read.
///
/// A refresh that returns an error is not the case under test: the loop logs
/// that and carries on. What ends the task without a word is a panic, and a
/// store that panics on `get` is the shortest way to one.
#[derive(Debug, krabka_domain_macros::TypeNameDisplay)]
struct PanicOnGetStore(object_store::memory::InMemory);

#[krabka_domain_macros::delegate_object_store(self.0)]
#[async_trait::async_trait]
impl ObjectStore for PanicOnGetStore {
    async fn get_opts(
        &self,
        _location: &object_store::path::Path,
        _options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        panic!("the frontier object could not be read");
    }
}

/// The refresher's death reaches its caller.
///
/// The task honours cancellation, so shutdown always looked fine, and that is
/// what hid this: a panic inside the loop ended the refresher, the querier
/// carried on answering from the frontier it last read, and nothing anywhere
/// said the frontier had stopped moving. The handle is what makes that
/// observable, so the function has to give it back and the caller has to be
/// able to join it.
#[tokio::test]
async fn the_frontier_refresher_hands_back_a_handle_that_carries_its_panic() {
    let token = CancellationToken::new();
    let handle = spawn_compaction_frontier_refresher(
        Arc::new(PanicOnGetStore(object_store::memory::InMemory::new())),
        ObjectPath::default(),
        SharedCompactionFrontier::default(),
        BufferedLogHotTail::default(),
        token.clone(),
        millis(1),
    );

    let joined = tokio::time::timeout(millis(2_000).to_std(), handle)
        .await
        .expect("a panicking refresher ends, so its handle resolves");

    let error = joined.expect_err("the refresher panicked, so the join must not succeed");
    assert!(error.is_panic());
    // Cancellation was never asked for: the task ended on its own, which is
    // exactly what a supervisor must be able to tell apart from shutdown.
    assert!(!token.is_cancelled());
}

//! An in-memory object store whose first puts fail, for the retry tests of
//! the block writer and the metrics compactor's flush.
//!
//! Both crates' unit tests reach this file with `#[path]`, so it depends only
//! on what [`hooked_store`] does.

use std::sync::atomic::{AtomicUsize, Ordering};

use object_store::{PutPayload, path::Path};

#[path = "hooked_store.rs"]
mod hooked_store;

use self::hooked_store::{HookedStore, StoreHooks};

// An in-memory store whose first puts fail. It dereferences to its
// [`FailingPuts`], so a test reads the attempt count through the store.
pub type FlakyPutStore = HookedStore<FailingPuts>;

// Fails a countdown of puts with one error, counting every attempt.
pub struct FailingPuts {
    // Puts still to refuse before puts succeed; `usize::MAX` refuses all.
    remaining_failures: AtomicUsize,
    attempts: AtomicUsize,
    error: fn() -> object_store::Error,
}

impl FailingPuts {
    // The puts a test made, failed or not.
    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

// A store whose first `failures` puts fail with `error`.
pub fn flaky_put_store(failures: usize, error: fn() -> object_store::Error) -> FlakyPutStore {
    HookedStore::new(FailingPuts {
        remaining_failures: AtomicUsize::new(failures),
        attempts: AtomicUsize::new(0),
        error,
    })
}

#[async_trait::async_trait]
impl StoreHooks for FailingPuts {
    const NAME: &'static str = "FlakyPutStore";

    async fn before_put(
        &self,
        _location: &Path,
        _payload: &PutPayload,
    ) -> object_store::Result<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                (left > 0).then(|| left - 1)
            })
            .is_ok()
        {
            return Err((self.error)());
        }
        Ok(())
    }
}

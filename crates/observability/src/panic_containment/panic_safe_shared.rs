use std::sync::{Mutex, PoisonError};

/// Shared state that a panic cannot leave half-updated, and whose poison
/// therefore carries no information.
///
/// `std::sync` poisons a lock when a thread unwinds while holding it
/// exclusively, because the value behind it may have been mutated part-way.
/// That is the right default, and `.expect("... lock poisoned")` on top of it
/// is the right reading of the default: the state may be wrong, so fail
/// loudly. The trouble is what it costs on a serving path. One panic in one
/// handler poisons the lock for the life of the process, and every later
/// request that touches the same state panics on the `expect`. One bad record
/// permanently breaks the role, and the role goes on listening.
///
/// This type removes the reason for the poison rather than the poison's
/// consequence. Every mutation runs on a private clone and is published into
/// the lock in a single move, which is the last thing [`update`] does and
/// cannot itself panic. An unwind inside the closure therefore drops the
/// half-written copy and leaves the shared value exactly as the last
/// successful update left it. Recovering the guard is then sound by
/// construction, not by assumption, and the next request sees consistent
/// state.
///
/// The price is one clone per update, so this is for state that is read far
/// more often than it is written -- rule groups, delete requests, a readiness
/// gate list -- and not for a per-request counter. Where the clone is too
/// expensive, keep `std::sync` and its loud poison instead, and make the
/// critical section unable to panic.
///
/// This is not a general escape from poisoning. A `Mutex<T>` whose critical
/// section mutates `T` in place has a real reason to poison, and clearing that
/// poison would hide corruption instead of preventing it, which is worse than
/// a crash because it is silent.
///
/// [`update`]: PanicSafeShared::update
pub struct PanicSafeShared<T> {
    inner: Mutex<T>,
}

impl<T> PanicSafeShared<T> {
    /// Shares `value`.
    pub const fn new(value: T) -> Self {
        Self {
            inner: Mutex::new(value),
        }
    }

    /// Reads the shared value.
    ///
    /// A panic inside `read` poisons the lock without mutating anything, so
    /// the recovery here needs no further justification than that `read`
    /// receives a shared reference.
    pub fn read<R>(&self, read: impl FnOnce(&T) -> R) -> R {
        let guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        read(&guard)
    }
}

impl<T: Clone> PanicSafeShared<T> {
    /// Applies `update` to a private copy of the shared value and publishes
    /// the result.
    ///
    /// The shared value changes only if `update` returns. If it panics, the
    /// copy goes with the unwind and the next reader sees the state from
    /// before the call.
    pub fn update<R>(&self, update: impl FnOnce(&mut T) -> R) -> R {
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let mut next = guard.clone();
        let outcome = update(&mut next);
        *guard = next;
        outcome
    }

    /// A copy of the shared value.
    pub fn snapshot(&self) -> T {
        self.read(Clone::clone)
    }
}

impl<T: Default> Default for PanicSafeShared<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

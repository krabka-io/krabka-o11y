use super::*;

/// Wall-clock source for cache-entry age checks.
///
/// The trait exists so that tests can advance time deterministically with
/// [`ManualClock`] instead of a sleep. Production uses [`SystemClock`].
pub trait Clock: Send + Sync {
    /// Current time as Unix-epoch milliseconds.
    fn now_epoch_millis(&self) -> i64;
}

use super::{Duration, NonZeroU32};

/// How many times, and how patiently, one object-store operation is retried
/// before the caller is told it failed.
///
/// The schedule is a parameter rather than a constant so a test can inject one
/// that does not sleep. Raising the production default then costs a test suite
/// nothing.
///
/// # The bound
///
/// `max_attempts` is deliberately small and there is no "retry forever" mode.
/// An unbounded retry turns a permanent misconfiguration -- a bucket that does
/// not exist, a credential that was revoked -- into a role that is up, silent
/// and doing nothing, which is a worse failure than the crash it replaced.
/// When the budget is spent the error is returned, the WAL offsets stay
/// uncommitted, and the role exits so its restart policy and its alerting see
/// the failure.
///
/// The budget is small for a second reason: it is not the only one. The
/// `object_store` HTTP clients carry their own [`RetryConfig`], which already
/// spends up to ten attempts and three minutes on a single request before it
/// reports a failure at all. This policy exists to re-drive the *whole
/// operation* after that budget is spent -- a fresh multipart upload, a
/// re-encoded block -- not to extend it.
///
/// [`RetryConfig`]: object_store::RetryConfig
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectStoreRetryPolicy {
    /// Total attempts, the first one included. One means "do not retry".
    pub max_attempts: NonZeroU32,
    /// How long to wait before the second attempt.
    pub initial_backoff: Duration,
    /// The ceiling the doubling backoff climbs to.
    pub max_backoff: Duration,
}

impl ObjectStoreRetryPolicy {
    /// Four attempts, 250 ms doubling, capped at 4 s.
    ///
    /// Worst case this adds 250 ms + 500 ms + 1 s of waiting to an operation
    /// that ends up failing anyway. That is long enough to ride out the
    /// failure modes object stores actually have -- a bucket partition moving,
    /// a load balancer draining, a burst of 503s -- and short enough that a
    /// real outage is reported in seconds rather than minutes. Four attempts
    /// never reach the 4 s cap; it is there so that raising `max_attempts`
    /// lengthens the budget without letting one wait grow without bound.
    pub const DEFAULT: Self = Self {
        max_attempts: NonZeroU32::new(4).expect("4 is not zero"),
        initial_backoff: Duration::from_millis(250),
        max_backoff: Duration::from_secs(4),
    };

    /// A policy with `max_attempts` attempts and no waiting between them.
    /// One attempt means nothing is retried.
    ///
    /// This is what tests inject: the retry behaviour is observable in the
    /// number of attempts the store sees, and the suite does not get slower
    /// when [`Self::DEFAULT`] is made more patient.
    ///
    /// # Panics
    /// Panics when `max_attempts` is zero.
    #[must_use]
    pub const fn immediate(max_attempts: u32) -> Self {
        Self {
            max_attempts: NonZeroU32::new(max_attempts).expect("max_attempts must not be zero"),
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }
}

impl Default for ObjectStoreRetryPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

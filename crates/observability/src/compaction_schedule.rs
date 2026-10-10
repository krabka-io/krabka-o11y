//! The tick a compactor role runs its passes on.
//!
//! Every signal's compactor runs one pass per interval until its role is shut
//! down. [`CompactionSchedule`] is that wait, so each loop holds only what its
//! own pass does.

use krabka_units::{Time, convert::TimeExt as _};
use tokio::time::{Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// Paces a compactor's passes and stops them at shutdown.
///
/// The tick skips a missed deadline rather than firing twice. A pass that ran
/// long has already read whatever the skipped tick would have read.
#[derive(Debug)]
pub struct CompactionSchedule {
    tick: Interval,
    shutdown: CancellationToken,
}

impl CompactionSchedule {
    /// A schedule whose first pass is due at once and every later one
    /// `interval` after the previous tick, until `shutdown` fires.
    ///
    /// # Panics
    ///
    /// Panics outside a Tokio runtime, or if `interval` is zero.
    #[must_use]
    pub fn new(interval: Time, shutdown: CancellationToken) -> Self {
        let mut tick = tokio::time::interval(interval.to_std());
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self { tick, shutdown }
    }

    /// Waits until the next pass is due, and returns `false` instead once
    /// shutdown has fired.
    ///
    /// Shutdown wins a tie with the tick, so a role that is stopping starts no
    /// further pass.
    pub async fn next_pass_due(&mut self) -> bool {
        tokio::select! {
            biased;
            () = self.shutdown.cancelled() => false,
            _ = self.tick.tick() => true,
        }
    }
}

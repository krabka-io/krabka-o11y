use std::{fmt, sync::Arc};

use qubit_clock::{
    Clock, SystemClock,
    sleep::{AsyncSleeper, SystemSleeper},
};

/// The clock that gives each audit event its time, and the sleeper that sets
/// the pace of the audit writer.
///
/// A service uses [`AuditClocks::system`]. A test gives a
/// `qubit_clock::MockTime` clock and sleeper, so event times and the writer's
/// checkpoint and replay timers move only when the test moves them.
#[derive(Clone)]
pub struct AuditClocks {
    /// Gives the epoch-millisecond time of each event.
    pub clock: Arc<dyn Clock>,
    /// Sets the pace of the writer's checkpoint timer and spool-replay timer.
    pub sleeper: Arc<dyn AsyncSleeper>,
}

impl AuditClocks {
    /// The system wall clock and real sleeps.
    #[must_use]
    pub fn system() -> Self {
        Self {
            clock: Arc::new(SystemClock::new()),
            sleeper: Arc::new(SystemSleeper::new()),
        }
    }
}

impl fmt::Debug for AuditClocks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditClocks")
            .finish_non_exhaustive()
    }
}

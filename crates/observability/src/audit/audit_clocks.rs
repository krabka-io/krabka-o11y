use std::{fmt, sync::Arc};

use qubit_clock::{StdTimer, StdWallClock, Timer, WallClock};

/// The clock that gives each audit event its time, and the timer that sets
/// the pace of the audit writer.
///
/// A service uses [`AuditClocks::system`]. A test gives a
/// `qubit_clock::ManualMonotonicClock` and its wall clock and timer, so event
/// times and the writer's checkpoint and replay timers move only when the test
/// moves them.
#[derive(Clone)]
pub struct AuditClocks {
    /// Gives the epoch-millisecond time of each event.
    pub clock: Arc<dyn WallClock>,
    /// Sets the pace of the writer's checkpoint timer and spool-replay timer.
    pub timer: Arc<dyn Timer>,
}

impl AuditClocks {
    /// The system wall clock and real sleeps.
    #[must_use]
    pub fn system() -> Self {
        Self {
            clock: Arc::new(StdWallClock::new()),
            timer: Arc::new(StdTimer::new()),
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

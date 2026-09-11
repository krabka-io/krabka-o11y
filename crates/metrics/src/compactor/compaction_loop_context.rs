use super::{CompactionClock, ServiceMetrics};

/// The collaborators a compactor loop consults on every iteration but does not
/// read its work from: the clock it measures flush age with, and the
/// instruments it records the poll and the flush into.
///
/// They travel together because they are the loop's ambient context rather
/// than its inputs. A test injects a clock that does not advance on its own and
/// a registry it can scrape, and it injects both the same way.
#[derive(Clone, Copy)]
pub struct CompactionLoopContext<'a, Clock: ?Sized> {
    /// The clock the loop reads to decide whether the buffer has aged out.
    pub clock: &'a Clock,
    /// The instruments the loop records its polls and its flushes into.
    pub metrics: &'a ServiceMetrics,
}

impl<'a, Clock> CompactionLoopContext<'a, Clock>
where
    Clock: CompactionClock + ?Sized,
{
    /// Builds a context from a clock and an instrument bundle.
    #[must_use]
    pub fn new(clock: &'a Clock, metrics: &'a ServiceMetrics) -> Self {
        Self { clock, metrics }
    }
}

use super::Instant;

/// What the distributor remembers about one series of one tenant.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SeriesActivity {
    /// Newest sample timestamp accepted for the series, in epoch
    /// milliseconds. `None` until the series carries a timestamped sample,
    /// which is what lets the first sample of a series be any age.
    pub(crate) latest_sample_ms: Option<i64>,
    /// Monotonic instant of the most recent write to the series. The idle
    /// sweep compares it against the tenant's idle timeout.
    pub(crate) last_seen: Instant,
}

impl SeriesActivity {
    pub(crate) const fn new(now: Instant) -> Self {
        Self {
            latest_sample_ms: None,
            last_seen: now,
        }
    }
}

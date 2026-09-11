use super::{BTreeMap, Instant, SeriesActivity, SeriesFingerprint, Time, TimeExt};

/// One tenant's series state: the active-series set and each series' newest
/// accepted sample, in one map.
#[derive(Clone, Debug)]
pub(crate) struct TenantSeries {
    pub(crate) series: BTreeMap<SeriesFingerprint, SeriesActivity>,
    /// Monotonic instant of this tenant's most recent write. The tracker
    /// evicts the least-recently-written tenant when it is over its cap.
    pub(crate) last_seen: Instant,
    /// The tenant's resolved idle timeout, stamped on every write so the sweep
    /// does not have to resolve the tenant's limits again.
    pub(crate) idle_timeout: Time,
}

impl TenantSeries {
    pub(crate) fn new(now: Instant, idle_timeout: Time) -> Self {
        Self {
            series: BTreeMap::new(),
            last_seen: now,
            idle_timeout,
        }
    }

    /// Records a write to `fingerprint` at `now` and returns its state.
    pub(crate) fn touch(
        &mut self,
        fingerprint: SeriesFingerprint,
        now: Instant,
    ) -> &mut SeriesActivity {
        self.series
            .entry(fingerprint)
            .and_modify(|activity| activity.last_seen = now)
            .or_insert_with(|| SeriesActivity::new(now))
    }

    /// Drops the series this tenant has not written to within its idle
    /// timeout. A zero timeout keeps every series that was ever written.
    pub(crate) fn sweep(&mut self, now: Instant) {
        if self.idle_timeout <= Time::ZERO {
            return;
        }
        let idle_timeout = self.idle_timeout.to_std();
        self.series
            .retain(|_, activity| now.duration_since(activity.last_seen) < idle_timeout);
    }
}

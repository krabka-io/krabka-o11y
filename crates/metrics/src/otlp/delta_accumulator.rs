use super::{
    BTreeMap, DELTA_PRUNE_INTERVAL, DeltaHistogramState, DeltaKey, DeltaState, Instant, Labels,
    Limits, NativeHistogram, OtlpError, Time, TimeExt, add_compatible_native_histogram, delta_key,
};

/// Stateful accumulator for OTLP delta-temporality sums and histograms.
///
/// One accumulator holds one tenant's streams. Two tenants that export the same
/// metric with the same attributes therefore keep separate cumulative values.
///
/// The state is in memory only. A distributor restart resets every delta stream
/// to zero, so the first point after a restart is the whole cumulative value
/// rather than a continuation. The upstream in-memory `deltatocumulative`
/// processor of the OpenTelemetry Collector behaves the same way.
#[derive(Clone, Debug)]
pub struct DeltaAccumulator {
    pub(crate) sums: BTreeMap<DeltaKey, DeltaState>,
    pub(crate) histograms: BTreeMap<DeltaKey, DeltaHistogramState>,
    /// The instant that stamps every stream the next decode touches.
    now: Instant,
    /// Instant at which the next stale pass may run.
    next_prune: Instant,
}

impl Default for DeltaAccumulator {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            sums: BTreeMap::new(),
            histograms: BTreeMap::new(),
            now,
            next_prune: now,
        }
    }
}

impl DeltaAccumulator {
    /// Sets the instant the streams of the next decode are seen at.
    pub(crate) fn seen_at(&mut self, now: Instant) {
        self.now = now;
    }

    /// Keeps the accumulated state within the tenant's limits.
    ///
    /// It drops the streams that took no point within `otlp_delta_max_stale`,
    /// at most once per [`DELTA_PRUNE_INTERVAL`], and then evicts the least
    /// recently seen streams while more than `otlp_delta_max_streams` remain.
    ///
    /// The upstream `deltatocumulative` processor refuses a *new* stream over
    /// its `max_streams` and keeps the ones it holds. Krabka evicts the least
    /// recently seen instead, so an exporter that rotates its attribute sets
    /// keeps working rather than stalling on streams nothing writes to.
    pub(crate) fn bound(&mut self, limits: &Limits) {
        if self.now >= self.next_prune {
            self.drop_stale(limits.otlp_delta_max_stale);
            self.next_prune = self.now + DELTA_PRUNE_INTERVAL.to_std();
        }
        self.evict_if_over_cap(limits.otlp_delta_max_streams);
    }

    /// Drops the streams that took no point within `max_stale`. A zero extent
    /// keeps every stream.
    fn drop_stale(&mut self, max_stale: Time) {
        if max_stale <= Time::ZERO {
            return;
        }
        let max_stale = max_stale.to_std();
        let now = self.now;
        self.sums
            .retain(|_, state| now.duration_since(state.last_seen) < max_stale);
        self.histograms
            .retain(|_, state| now.duration_since(state.last_seen) < max_stale);
    }

    /// Evicts the least recently seen streams until at most `max_streams`
    /// remain. A cap of `0` turns the eviction off.
    ///
    /// This runs only on the cold path, where the tenant is already at its
    /// stream cap. `max_streams` bounds the linear scan, and the scan never
    /// runs on the steady-state hot path.
    fn evict_if_over_cap(&mut self, max_streams: u64) {
        if max_streams == 0 {
            return;
        }
        let max_streams = usize::try_from(max_streams).unwrap_or(usize::MAX);
        while self.sums.len() + self.histograms.len() > max_streams {
            let oldest_sum = self
                .sums
                .iter()
                .min_by_key(|(_, state)| state.last_seen)
                .map(|(key, state)| (key.clone(), state.last_seen));
            let oldest_histogram = self
                .histograms
                .iter()
                .min_by_key(|(_, state)| state.last_seen)
                .map(|(key, state)| (key.clone(), state.last_seen));
            match (oldest_sum, oldest_histogram) {
                (Some((sum_key, sum_seen)), Some((histogram_key, histogram_seen))) => {
                    if sum_seen <= histogram_seen {
                        self.sums.remove(&sum_key);
                    } else {
                        self.histograms.remove(&histogram_key);
                    }
                }
                (Some((key, _)), None) => {
                    self.sums.remove(&key);
                }
                (None, Some((key, _))) => {
                    self.histograms.remove(&key);
                }
                (None, None) => break,
            }
        }
    }

    pub(crate) fn accumulate_sum(
        &mut self,
        labels: &Labels,
        start_time_unix_nano: u64,
        delta: f64,
    ) -> f64 {
        let key = delta_key(labels);
        let now = self.now;
        let state = self.sums.entry(key).or_insert_with(|| DeltaState::new(now));
        state.last_seen = now;
        if start_time_unix_nano != 0
            && state.start_time_unix_nano != 0
            && state.start_time_unix_nano != start_time_unix_nano
        {
            state.value = delta;
        } else {
            state.value += delta;
        }
        if start_time_unix_nano != 0 {
            state.start_time_unix_nano = start_time_unix_nano;
        }
        state.value
    }

    pub(crate) fn accumulate_histogram(
        &mut self,
        metric_name: &str,
        labels: &Labels,
        start_time_unix_nano: u64,
        delta: NativeHistogram,
    ) -> Result<NativeHistogram, OtlpError> {
        let key = delta_key(labels);
        let now = self.now;
        let state = self
            .histograms
            .entry(key)
            .or_insert_with(|| DeltaHistogramState::new(now));
        state.last_seen = now;
        if start_time_unix_nano != 0
            && state.start_time_unix_nano != 0
            && state.start_time_unix_nano != start_time_unix_nano
        {
            state.value = Some(delta);
        } else if let Some(cumulative) = &mut state.value {
            add_compatible_native_histogram(metric_name, cumulative, &delta)?;
        } else {
            state.value = Some(delta);
        }
        if start_time_unix_nano != 0 {
            state.start_time_unix_nano = start_time_unix_nano;
        }
        state.value.clone().ok_or_else(|| {
            OtlpError::Invalid(metric_name.into(), "missing accumulated histogram".into())
        })
    }
}

use super::*;

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

struct OldestStream {
    key: DeltaKey,
    last_seen: Instant,
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
                .min_by_key(|entry| entry.1.last_seen)
                .map(|entry| OldestStream {
                    key: entry.0.clone(),
                    last_seen: entry.1.last_seen,
                });
            let oldest_histogram = self
                .histograms
                .iter()
                .min_by_key(|entry| entry.1.last_seen)
                .map(|entry| OldestStream {
                    key: entry.0.clone(),
                    last_seen: entry.1.last_seen,
                });
            match oldest_sum {
                Some(sum) => match oldest_histogram {
                    Some(histogram) if histogram.last_seen < sum.last_seen => {
                        self.histograms.remove(&histogram.key);
                    }
                    Some(_) | None => {
                        self.sums.remove(&sum.key);
                    }
                },
                None => match oldest_histogram {
                    Some(histogram) => {
                        self.histograms.remove(&histogram.key);
                    }
                    None => break,
                },
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

#[derive(Clone, Debug)]
pub(crate) struct DeltaHistogramState {
    pub(crate) start_time_unix_nano: u64,
    pub(crate) value: Option<NativeHistogram>,
    /// Monotonic instant of the last point folded into this stream. The
    /// accumulator drops the stream once it is stale.
    pub(crate) last_seen: Instant,
}

impl DeltaHistogramState {
    pub(crate) const fn new(now: Instant) -> Self {
        Self {
            start_time_unix_nano: 0,
            value: None,
            last_seen: now,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaKey {
    pub(crate) labels: Vec<DeltaLabel>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeltaLabel {
    pub(crate) name: String,
    pub(crate) value: String,
}

pub(crate) fn delta_key(labels: &Labels) -> DeltaKey {
    DeltaKey {
        labels: labels
            .iter()
            .map(|label| DeltaLabel {
                name: label.name.to_owned(),
                value: label.value.to_owned(),
            })
            .collect(),
    }
}

/// How often an accumulator looks for stale delta streams to drop.
///
/// The pass walks every stream the tenant has, so it must not run per request.
/// It runs at most once per this interval, over a map the stream cap already
/// bounds. A stream therefore survives `otlp_delta_max_stale` by up to one
/// interval.
pub(crate) const DELTA_PRUNE_INTERVAL: Time = minutes(1);

#[derive(Clone, Copy, Debug)]
pub(crate) struct DeltaState {
    pub(crate) start_time_unix_nano: u64,
    pub(crate) value: f64,
    /// Monotonic instant of the last point folded into this stream. The
    /// accumulator drops the stream once it is stale.
    pub(crate) last_seen: Instant,
}

impl DeltaState {
    pub(crate) const fn new(now: Instant) -> Self {
        Self {
            start_time_unix_nano: 0,
            value: 0.0,
            last_seen: now,
        }
    }
}

/// One tenant's delta accumulator, with a monotonic last-touch stamp. The
/// holder uses that stamp for least-recently-used eviction once it reaches its
/// tenant cap.
#[derive(Debug)]
pub(crate) struct TenantDeltaAccumulator {
    pub(crate) accumulator: Arc<Mutex<DeltaAccumulator>>,
    pub(crate) last_touch: AtomicU64,
}

/// The OTLP delta accumulators, one per tenant.
///
/// A single process-wide accumulator would fold two tenants' deltas for the
/// same attribute set into one cumulative value, which is both a cross-tenant
/// leak and a wrong value for both. It would also serialise the OTLP decode of
/// every tenant behind one lock. One accumulator per tenant fixes all three,
/// and gives each tenant its own place to bound.
#[derive(Debug)]
pub struct TenantDeltaAccumulators {
    accumulators: DashMap<TenantId, TenantDeltaAccumulator>,
    /// Maximum number of distinct tenants that hold an accumulator.
    max_tenants: usize,
    /// Monotonic logical clock that stamps touches for LRU eviction.
    touch_clock: AtomicU64,
}

impl TenantDeltaAccumulators {
    /// Builds a holder for at most `max_tenants` tenants. A cap of `0` clamps
    /// to `1`.
    #[must_use]
    pub fn new(max_tenants: usize) -> Self {
        Self {
            accumulators: DashMap::new(),
            max_tenants: max_tenants.max(1),
            touch_clock: AtomicU64::new(0),
        }
    }

    /// Returns `tenant`'s accumulator, and creates one when the tenant is new.
    #[must_use]
    pub(crate) fn for_tenant(&self, tenant: &TenantId) -> Arc<Mutex<DeltaAccumulator>> {
        let stamp = self.touch_clock.fetch_add(1, Ordering::Relaxed);
        let entry =
            self.accumulators
                .entry(tenant.clone())
                .or_insert_with(|| TenantDeltaAccumulator {
                    accumulator: Arc::new(Mutex::new(DeltaAccumulator::default())),
                    last_touch: AtomicU64::new(stamp),
                });
        // Stamp this access for LRU eviction, then drop the dashmap entry guard
        // before scanning so the eviction sweep never contends with the shard
        // lock this tenant lives in.
        entry.last_touch.store(stamp, Ordering::Relaxed);
        let accumulator = Arc::clone(&entry.accumulator);
        drop(entry);
        self.evict_if_over_cap();
        accumulator
    }

    /// Evicts the least-recently-touched tenants until the map is within the
    /// cap.
    ///
    /// This runs only on the cold path, where a new tenant arrives while the
    /// map is already at capacity. `max_tenants` bounds the linear scan, and
    /// the scan never runs on the steady-state hot path.
    fn evict_if_over_cap(&self) {
        crate::least_recently_touched::evict_until_within_cap(
            &self.accumulators,
            self.max_tenants,
            |accumulator| accumulator.last_touch.load(Ordering::Relaxed),
        );
    }

    #[cfg(test)]
    pub(crate) fn tenant_count(&self) -> usize {
        self.accumulators.len()
    }
}

pub(crate) fn accumulate_delta_float_series(
    series: &mut [DecodedSeries],
    start_time_unix_nano: u64,
    accumulator: &mut DeltaAccumulator,
) {
    for series in series {
        for sample in &mut series.samples {
            sample.value =
                accumulator.accumulate_sum(&series.labels, start_time_unix_nano, sample.value);
            if start_time_unix_nano != 0 {
                sample.start_timestamp_ms = Some(nanos_to_millis(start_time_unix_nano));
            }
        }
    }
}

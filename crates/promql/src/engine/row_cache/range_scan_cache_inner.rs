use super::{Arc, BTreeMap, HashMap, Labels, ScannedRows, SeriesFingerprint, Windowed};

#[derive(Clone, Copy)]
pub(crate) struct TimeWindow {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
}

/// Per-query rows and labels, retained for bounded selector union windows.
/// Each subquery uses a separate cache for its own evaluation grid.
pub(crate) struct RangeScanCacheInner {
    /// A single instant leaf can use latest samples without changing the
    /// shared full-scan snapshot seen by composite or range expressions.
    pub(crate) allow_latest_float_samples: bool,
    /// The window that a range query scans for each matcher set, or `None`
    /// for an instant query.
    pub(crate) unions: Option<HashMap<String, TimeWindow>>,
    // Dynamically generated selectors retain the ordinary lookback window.
    fallback: Option<TimeWindow>,
    /// Per-matcher-set float and histogram rows.
    pub(crate) rows: HashMap<String, Windowed<ScannedRows>>,
    /// Per-matcher-set fingerprint->labels resolution. A series label set is
    /// immutable, so the result for a wider window is a superset of the active
    /// series of any sub-window. Callers use it only as a `get(&fp)` lookup
    /// keyed by rows already filtered to the sub-window, so they never read the
    /// extra entries.
    pub(crate) labels: HashMap<String, Windowed<BTreeMap<SeriesFingerprint, Arc<Labels>>>>,
}

impl RangeScanCacheInner {
    pub(crate) fn range(unions: HashMap<String, TimeWindow>, fallback: Option<TimeWindow>) -> Self {
        let mut cache = Self::new(Some(unions));
        cache.fallback = fallback;
        cache
    }

    /// A cache for an instant query, which keeps the window of each scan.
    pub(crate) fn instant() -> Self {
        Self::new(None)
    }

    fn new(unions: Option<HashMap<String, TimeWindow>>) -> Self {
        Self {
            allow_latest_float_samples: false,
            unions,
            fallback: None,
            rows: HashMap::new(),
            labels: HashMap::new(),
        }
    }

    /// The window to scan and keep for a request of `[start_ms, end_ms]`, or
    /// `None` when the result of the request is not to be kept.
    pub(crate) fn fill_window(&self, key: &str, start_ms: i64, end_ms: i64) -> Option<TimeWindow> {
        match &self.unions {
            None => Some(TimeWindow { start_ms, end_ms }),
            Some(unions) => unions
                .get(key)
                .copied()
                .or(self.fallback)
                .filter(|union| start_ms >= union.start_ms && end_ms <= union.end_ms),
        }
    }
}
